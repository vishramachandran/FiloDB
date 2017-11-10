package filodb.coordinator

import scala.collection.mutable.HashMap
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.util.control.NonFatal
import scala.util.Try

import akka.actor.{ActorRef, Props}
import akka.event.LoggingReceive
import monix.execution.{CancelableFuture, Scheduler}
import monix.reactive.Observable
import net.ceedubs.ficus.Ficus._

import filodb.core.memstore._
import filodb.core.metadata.Dataset
import filodb.core.{DatasetRef, Iterators}

object IngestionActor {
  final case class IngestRows(ackTo: ActorRef, shard: Int, records: Seq[IngestRecord])

  case object GetStatus

  final case class IngestionStatus(rowsIngested: Long)

  def props(dataset: Dataset,
            memStore: MemStore,
            source: NodeClusterActor.IngestionSource,
            shardActor: ActorRef)(implicit sched: Scheduler): Props =
    Props(classOf[IngestionActor], dataset, memStore, source, shardActor, sched)
}

/**
  * Oversees ingestion and recovery process for a single dataset.  The overall process for a single shard:
  * 1. StartShardIngestion command is received and start() called
  * 2. MemStore.setup() is called for that shard
  * 3. IF no checkpoint data is found, THEN normal ingestion is started
  * 4. IF checkpoints are found, then recovery is started from the minimum checkpoint offset
  *    and goes until the maximum checkpoint offset.  These offsets are per subgroup of the shard.
  *    Progress will be sent at regular intervals
  * 5. Once the recovery has proceeded beyond the end checkpoint then normal ingestion is started
  *
  * ERROR HANDLING: currently any error in ingestion stream or memstore ingestion wll stop the ingestion
  *
  * @param sched a Scheduler for running ingestion stream Observables
  */
private[filodb] final class IngestionActor(dataset: Dataset,
                                           memStore: MemStore,
                                           source: NodeClusterActor.IngestionSource,
                                           shardActor: ActorRef)
                                          (implicit sched: Scheduler) extends BaseActor {

  import IngestionActor._

  final val streamSubscriptions = new HashMap[Int, CancelableFuture[Unit]]
  final val streams = new HashMap[Int, IngestionStream]

  // Params for creating the default memStore flush scheduler
  // TODO: eventually make the flush scheduler pluggable based on the source config
  private final val chunkDuration = source.config.as[Option[FiniteDuration]]("chunk-duration")
                                                 .map(_ / 1000).getOrElse(1.hour)

  // TODO: add and remove per-shard ingestion sources?
  // For now just start it up one time and kill the actor if it fails
  val ctor = Class.forName(source.streamFactoryClass).getConstructors.head
  val streamFactory = ctor.newInstance().asInstanceOf[IngestionStreamFactory]
  logger.info(s"Using stream factory $streamFactory with config ${source.config}")

  override def postStop(): Unit = {
    super.postStop() // <- logs shutting down
    logger.info("Cancelling all streams and calling teardown")
    streamSubscriptions.keys.foreach(stop(dataset.ref, _, ActorRef.noSender))
  }

  /** All [[ShardCommand]] tasks are only started if the dataset
    * and shard are valid for this ingester.
    */
  def receive: Receive = LoggingReceive {
    case e: StartShardIngestion        => start(e, sender())
    case e: IngestRows                 => ingest(e)
    case GetStatus                     => status(sender())
    case StopShardIngestion(ds, shard) => stop(ds, shard, sender())
  }

  /** Guards that only this dataset's commands are acted upon.
    * Handles initial memstore setup of dataset to shard.
    * Also handles recovery process.
    */
  private def start(e: StartShardIngestion, origin: ActorRef): Unit =
    if (invalid(e.ref)) handleInvalid(e, Some(origin)) else {
      try memStore.setup(dataset, e.shard) catch {
        case ex@DatasetAlreadySetup(ds) =>
          logger.warn(s"Dataset $ds already setup", ex)
        case ShardAlreadySetup =>
          logger.warn(s"Shard already setup")
      }

      for { checkpoints <- memStore.metastore.readCheckpoints(dataset.ref, e.shard) }
      yield {
        if (checkpoints.isEmpty) {
          // Start normal ingestion with no recovery checkpoint and flush group 0 first
          normalIngestion(e.shard, None, 0)
        } else {
          // Figure out recovery end watermark and intervals.  The reportingInterval is the interval at which
          // offsets come back from the MemStore for us to report progress.
          val startRecoveryWatermark = checkpoints.values.min
          val endRecoveryWatermark = checkpoints.values.max
          val lastFlushedGroup = checkpoints.find(_._2 == endRecoveryWatermark).get._1
          val reportingInterval = Math.max((endRecoveryWatermark - startRecoveryWatermark) / 20, 1L)
          logger.info(s"Starting recovery: from $startRecoveryWatermark to $endRecoveryWatermark; " +
                      s"last flushed group $lastFlushedGroup")
          for { lastOffset <- doRecovery(e.shard, startRecoveryWatermark, endRecoveryWatermark, reportingInterval,
                                         checkpoints) }
          yield {
            // Start reading past last offset for normal records; start flushes one group past last group
            normalIngestion(e.shard, Some(lastOffset + 1), (lastFlushedGroup + 1) % memStore.numGroups)
          }
        }
      }
    }

  private def flushStream(startGroupNo: Int = 0): Observable[FlushCommand] = {
    if (source.config.as[Option[Boolean]]("noflush").getOrElse(false)) {
      FlushStream.empty
    } else {
      FlushStream.interval(memStore.numGroups, chunkDuration / memStore.numGroups, startGroupNo)
    }
  }

  /**
   * Initiates post-recovery ingestion and regular flushing from the source.
   * startingGroupNo and offset would be defined for recovery scenarios.
   * @param shard the shard number to start ingestion
   * @param offset optionally the offset to start ingestion at
   * @param startingGroupNo the group number to start flushes at
   */
  private def normalIngestion(shard: Int, offset: Option[Long], startingGroupNo: Int): Unit = {
    create(shard, offset) map { ingestionStream =>
      val stream = ingestionStream.get
      shardActor ! IngestionStarted(dataset.ref, shard, context.parent)

      streamSubscriptions(shard) = memStore.ingestStream(dataset.ref, shard, stream, flushStream(startingGroupNo)) {
        ex => handleError(dataset.ref, shard, ex)
      }
      // On completion of the future, send IngestionStopped
      // except for noOpSource, which would stop right away, and is used for sending in tons of data
      streamSubscriptions(shard).foreach { x =>
        if (source != NodeClusterActor.noOpSource) shardActor ! IngestionStopped(dataset.ref, shard)
        ingestionStream.teardown()
      }
    } recover { case NonFatal(t) =>
      handleError(dataset.ref, shard, t)
    }
  }

  import Iterators._

  /**
   * Starts the recovery stream; returns the last offset read during recovery process
   * Periodically (every interval offsets) reports recovery progress
   * This stream is optimized for recovery; no flushes or other write I/O is performed.
   * @param shard the shard number to start recovery on
   * @param startOffset the starting offset to begin recovery
   * @param endOffset the offset past which recovery should stop (approximately)
   * @param interval the interval of reporting progress
   */
  private def doRecovery(shard: Int, startOffset: Long, endOffset: Long, interval: Long,
                         checkpoints: Map[Int, Long]): Future[Long] = {

    val futTry = create(shard, Some(startOffset)) map { ingestionStream =>
      val stream = ingestionStream.get
      shardActor ! RecoveryStarted(dataset.ref, shard, context.parent, 0)

      val fut = memStore.recoverStream(dataset.ref, shard, stream, checkpoints, interval)
        .map { off =>
          val progressPct = (off - startOffset) * 100 / (endOffset - startOffset)
          shardActor ! RecoveryStarted(dataset.ref, shard, context.parent, progressPct.toInt)
          off }
        .until(_ >= endOffset)
        .lastL.runAsync
      fut.foreach { off => ingestionStream.teardown() }
      fut.recover { case ex =>
        ingestionStream.teardown()
        handleError(dataset.ref, shard, ex)
      }
      fut
    }
    futTry.recover { case NonFatal(t) =>
      handleError(dataset.ref, shard, t)
      Future.failed(t)
    }
    futTry.get
  }

  /** [[filodb.coordinator.IngestionStreamFactory.create]] can raise IllegalArgumentException
    * if the shard is not 0. This will notify versus throw so the sender can handle the
    * problem, which is internal.
    */
  private def create(shard: Int, offset: Option[Long]): Try[IngestionStream] =
    Try {
      val ingestStream = streamFactory.create(source.config, dataset, shard, offset)
      streams(shard) = ingestStream
      logger.info(s"Ingestion stream $ingestStream set up for shard $shard")
      ingestStream
    }

  private def ingest(e: IngestRows): Unit = {
    memStore.ingest(dataset.ref, e.shard, e.records)
    if (e.records.nonEmpty) {
      e.ackTo ! IngestionCommands.Ack(e.records.last.offset)
    }
  }

  private def status(origin: ActorRef): Unit =
    origin ! IngestionStatus(memStore.numRowsIngested(dataset.ref))

  /** Guards that only this dataset's commands are acted upon. */
  private def stop(ds: DatasetRef, shard: Int, origin: ActorRef): Unit =
    if (invalid(ds)) handleInvalid(StopShardIngestion(ds, shard), Some(origin)) else {
      streamSubscriptions.remove(shard).foreach(_.cancel)
      streams.remove(shard).foreach(_.teardown())
      shardActor ! IngestionStopped(dataset.ref, shard)

      // TODO: release memory for shard in MemStore
      logger.info(s"Stopped streaming ingestion for shard $shard and released resources")
  }

  private def invalid(ref: DatasetRef): Boolean = ref != dataset.ref

  private def handleError(ref: DatasetRef, shard: Int, err: Throwable): Unit = {
    shardActor ! IngestionError(ref, shard, err)
    logger.error("Exception thrown during ingestion stream", err)
  }

  private def handleInvalid(command: ShardCommand, origin: Option[ActorRef]): Unit = {
    logger.error(s"$command is invalid for this ingester '${dataset.ref}'.")
    origin foreach(_ ! InvalidIngestionCommand(command.ref, command.shard))
  }

  private def recover(): Unit = {
    // TODO: start recovery, then.. could also be in Actor.preRestart()
    // statusActor ! RecoveryStarted(dataset.ref, shard, context.parent)
  }
}