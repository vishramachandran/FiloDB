package filodb.core.memstore

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext

import com.typesafe.config.Config
import debox.Buffer
import java.util
import kamon.Kamon
import kamon.trace.Span
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.{Observable, OverflowStrategy}

import filodb.core.{DatasetRef, Types}
import filodb.core.binaryrecord2.RecordSchema
import filodb.core.memstore.ratelimit.QuotaSource
import filodb.core.metadata.Schemas
import filodb.core.query.{QuerySession, ServiceUnavailableException}
import filodb.core.store._
import filodb.memory.NativeMemoryManager

/**
 * Extends TimeSeriesShard with on-demand paging functionality by populating in-memory partitions with chunks from
 * a raw chunk source which implements RawChunkSource.readRawPartitions API.
 */
class OnDemandPagingShard(ref: DatasetRef,
                          schemas: Schemas,
                          storeConfig: StoreConfig,
                          quotaSource: QuotaSource,
                          shardNum: Int,
                          bufferMemoryManager: NativeMemoryManager,
                          rawStore: ColumnStore,
                          metastore: MetaStore,
                          evictionPolicy: TimeSeriesEvictionPolicy,
                          filodbConfig: Config)
                         (implicit ec: ExecutionContext) extends
TimeSeriesShard(ref, schemas, storeConfig, quotaSource, shardNum, bufferMemoryManager, rawStore,
                metastore, evictionPolicy, filodbConfig)(ec) {
  import TimeSeriesShard._
  import FiloSchedulers._

  private val singleThreadPool =
    Scheduler.singleThread(s"${FiloSchedulers.PopulateChunksSched}-$ref-$shardNum")
  // TODO: make this configurable
  private val strategy = OverflowStrategy.BackPressure(1000)

  private def startODPSpan(): Span = Kamon.spanBuilder(s"odp-cassandra-latency")
    .asChildOf(Kamon.currentSpan())
    .tag("dataset", ref.dataset)
    .tag("shard", shardNum)
    .start()

  private def capDataScannedPerShardCheck(lookup: TsLookupResult): Unit = {
    lookup.firstSchemaId.foreach { schId =>
      lookup.chunkMethod match {
        case TimeRangeChunkScan(st, end) =>
          val numMatches = lookup.tsInMemory.length + lookup.tsIdsNotInMemory.length
          schemas.ensureQueriedDataSizeWithinLimitApprox(schId, numMatches,
            storeConfig.flushInterval.toMillis,
            storeConfig.estimatedIngestResolutionMillis, end - st, storeConfig.maxDataPerShardQuery)
        case _ =>
      }
    }
  }

  // NOTE: the current implementation is as follows
  //  1. Fetch partitions from memStore
  //  2. Determine, one at a time, what chunks are missing and could be fetched from disk
  //  3. Fetch missing chunks through a SinglePartitionScan
  //  4. upload to memory and return partition
  // Definitely room for improvement, such as fetching multiple partitions at once, more parallelism, etc.
  //scalastyle:off
  override def scanTimeSeries(partLookupRes: TsLookupResult,
                              colIds: Seq[Types.ColumnId],
                              querySession: QuerySession): Observable[ReadableTimeSeries] = {

    capDataScannedPerShardCheck(partLookupRes)

    // For now, always read every data column.
    // 1. We don't have a good way to update just some columns of a chunkset for ODP
    // 2. Timestamp column almost always needed
    // 3. We don't have a safe way to prevent JVM crashes if someone reads a column that wasn't paged in

    // 1. Fetch partitions from memstore
    val partIdsNotInMemory = partLookupRes.tsIdsNotInMemory

    // 2. Now determine list of partitions to ODP and the time ranges to ODP
    val partKeyBytesToPage = new ArrayBuffer[Array[Byte]]()
    val pagingMethods = new ArrayBuffer[ChunkScanMethod]
    val inMemOdp = debox.Set.empty[Int]

    partLookupRes.tsIdsMemTimeGap.foreach { case (pId, startTime) =>
      val p = tsIdToTsMap.get(pId)
      if (p != null) {
        val odpChunkScan = chunksToODP(p, partLookupRes.chunkMethod, pagingEnabled, startTime)
        odpChunkScan.foreach { rawChunkMethod =>
          pagingMethods += rawChunkMethod // TODO: really determine range for all partitions
          partKeyBytesToPage += p.tsKeyBytes
          inMemOdp += p.tsId
        }
      } else {
        // in the very rare case that partition literally *just* got evicted
        // we do not want to thrash by paging this partition back in.
        logger.warn(s"Skipped ODP of partId=$pId in dataset=$ref " +
          s"shard=$shardNum since we are very likely thrashing")
      }
    }
    logger.debug(s"Query on dataset=$ref shard=$shardNum resulted in partial ODP of partIds ${inMemOdp}, " +
      s"and full ODP of partIds ${partLookupRes.tsIdsNotInMemory}")

    // partitions that do not need ODP are those that are not in the inMemOdp collection
    val inMemParts = InMemTimeSeriesIterator2(partLookupRes.tsInMemory)
    val noOdpPartitions = inMemParts.filterNot(p => inMemOdp(p.tsId))

    // NOTE: multiPartitionODP mode does not work with AllChunkScan and unit tests; namely missing partitions will not
    // return data that is in memory.  TODO: fix
    val result = Observable.fromIterator(noOdpPartitions) ++ {
      if (storeConfig.multiPartitionODP) {
        Observable.fromTask(odpPartTask(partIdsNotInMemory, partKeyBytesToPage, pagingMethods,
                                        partLookupRes.chunkMethod)).flatMap { odpParts =>
          val multiPart = MultiTimeseriesScan(partKeyBytesToPage, shardNum)
          if (partKeyBytesToPage.nonEmpty) {
            val span = startODPSpan()
            rawStore.readRawPartitions(ref, maxChunkTime, multiPart, computeBoundingMethod(pagingMethods))
              // NOTE: this executes the partMaker single threaded.  Needed for now due to concurrency constraints.
              // In the future optimize this if needed.
              .mapAsync { rawPart => odpChunkStore.populateRawChunks(rawPart).executeOn(singleThreadPool) }
              .asyncBoundary(strategy) // This is needed so future computations happen in a different thread
              .doOnTerminate(ex => span.finish())
          } else { Observable.empty }
        }
      } else {
        Observable.fromTask(odpPartTask(partIdsNotInMemory, partKeyBytesToPage, pagingMethods,
                                        partLookupRes.chunkMethod)).flatMap { odpParts =>
          assertThreadName(QuerySchedName)
          logger.debug(s"Finished creating full ODP partitions ${odpParts.map(_.tsId)}")
          if(logger.underlying.isDebugEnabled) {
            partKeyBytesToPage.zip(pagingMethods).foreach { case (pk, method) =>
              logger.debug(s"Paging in chunks for partId=${getTimeSeries(pk).get.tsId} chunkMethod=$method")
            }
          }
          if (partKeyBytesToPage.nonEmpty) {
            val span = startODPSpan()
            Observable.fromIterable(partKeyBytesToPage.zip(pagingMethods))
              .mapAsync(storeConfig.demandPagingParallelism) { case (partBytes, method) =>
                rawStore.readRawPartitions(ref, maxChunkTime, SingleTimeseriesScan(partBytes, shardNum), method)
                  .mapAsync { rawPart => odpChunkStore.populateRawChunks(rawPart).executeOn(singleThreadPool) }
                  .asyncBoundary(strategy) // This is needed so future computations happen in a different thread
                  .defaultIfEmpty(getTimeSeries(partBytes).get)
                  .headL
                  // headL since we are fetching a SinglePartition above
              }
              .doOnTerminate(ex => span.finish())
          } else {
            Observable.empty
          }
        }
      }
    }
    result.map { p =>
      shardStats.numTsQueried.increment()
      p
    }
  }

  // 3. Deal with partitions no longer in memory but still indexed in Lucene.
  //    Basically we need to create TSPartitions for them in the ingest thread -- if there's enough memory
  private def odpPartTask(partIdsNotInMemory: Buffer[Int], partKeyBytesToPage: ArrayBuffer[Array[Byte]],
                          pagingMethods: ArrayBuffer[ChunkScanMethod], chunkMethod: ChunkScanMethod) =
  if (partIdsNotInMemory.nonEmpty) {
    createODPPartitionsTask(partIdsNotInMemory, { case (pId, pkBytes) =>
      partKeyBytesToPage += pkBytes
      pagingMethods += chunkMethod
      logger.debug(s"Finished creating part for full odp. Now need to page partId=$pId chunkMethod=$chunkMethod")
      shardStats.numTsRestored.increment()
    }).executeOn(ingestSched).asyncBoundary
    // asyncBoundary above will cause subsequent map operations to run on designated scheduler for task or observable
    // as opposed to ingestSched

  // No need to execute the task on ingestion thread if it's empty / no ODP partitions
  } else Task.now(Nil)

  /**
   * Creates a Task which is meant ONLY TO RUN ON INGESTION THREAD
   * to create TSPartitions for partIDs found in Lucene but not in in-memory data structures
   * It runs in ingestion thread so it can correctly verify which ones to actually create or not
   */
  private def createODPPartitionsTask(partIDs: Buffer[Int], callback: (Int, Array[Byte]) => Unit):
                                                                  Task[Seq[TimeSeries]] = Task {
    assertThreadName(IngestSchedName)
    require(partIDs.nonEmpty)
    partIDs.map { id =>
      // for each partID: look up in partitions
      tsIdToTsMap.get(id) match {
        case TimeSeriesShard.OutOfMemTs =>
          logger.debug(s"Creating TSPartition for ODP from part ID $id in dataset=$ref shard=$shardNum")
          // If not there, then look up in Lucene and get the details
          for { partKeyBytesRef <- tsKeyTagValueIndex.tsKeyFromTsId(id)
                unsafeKeyOffset = TsKeyLuceneIndex.bytesRefToUnsafeOffset(partKeyBytesRef.offset)
                group = tsKeyGroup(schemas.ts.binSchema, partKeyBytesRef.bytes, unsafeKeyOffset, numGroups)
                sch  <- Option(schemas(RecordSchema.schemaID(partKeyBytesRef.bytes, unsafeKeyOffset)))
                          } yield {
            val part = createNewTimeSeries(partKeyBytesRef.bytes, unsafeKeyOffset, group, id, sch, true, 4)
            if (part == OutOfMemTs) throw new ServiceUnavailableException("The server has too many ingesting " +
              "time series and does not have resources to serve this long time range query. Please try " +
              "after sometime.")
            val stamp = tsSetLock.writeLock()
              try {
                markTsAsNotIngesting(part, odp = true)
                tsKeyToTs.add(part)
              } finally {
                tsSetLock.unlockWrite(stamp)
              }
              val pkBytes = util.Arrays.copyOfRange(partKeyBytesRef.bytes, partKeyBytesRef.offset,
                partKeyBytesRef.offset + partKeyBytesRef.length)
              callback(part.tsId, pkBytes)
              part
            }
          // create the partition and update data structures (but no need to add to Lucene!)
          // NOTE: if no memory, then no partition!
        case p: TimeSeries =>
          // invoke callback even if we didn't create the partition
          callback(p.tsId, p.tsKeyBytes)
          Some(p)
      }
    }.toVector.flatten
  }

  private def computeBoundingMethod(methods: Seq[ChunkScanMethod]): ChunkScanMethod = if (methods.isEmpty) {
    AllChunkScan
  } else {
    var minTime = Long.MaxValue
    var maxTime = 0L
    methods.foreach { m =>
      minTime = Math.min(minTime, m.startTime)
      maxTime = Math.max(maxTime, m.endTime)
    }
    TimeRangeChunkScan(minTime, maxTime)
  }

  /**
    * Check if ODP is really needed for this partition which is in memory
    * @return Some(scanMethodForCass) if ODP is needed, None if ODP is not needed
    */
  private def chunksToODP(partition: ReadableTimeSeries,
                          method: ChunkScanMethod,
                          enabled: Boolean,
                          partStartTime: Long): Option[ChunkScanMethod] = {
    if (enabled) {
      method match {
        // For now, allChunkScan will always load from disk.  This is almost never used, and without an index we have
        // no way of knowing what there is anyways.
        case AllChunkScan                 =>  Some(AllChunkScan)
        // Assume initial startKey of first chunk is the earliest - typically true unless we load in historical data
        // Compare desired time range with start key and see if in memory data covers desired range
        // Also assume we have in memory all data since first key.  Just return the missing range of keys.
        case req: TimeRangeChunkScan      =>  if (partition.numChunks > 0) {
                                                val memStartTime = partition.earliestTime
                                                if (req.startTime < memStartTime && partStartTime < memStartTime) {
                                                  val toODP = TimeRangeChunkScan(req.startTime, memStartTime)
                                                  logger.debug(s"Decided to ODP time range $toODP for " +
                                                    s"partID=${partition.tsId} memStartTime=$memStartTime " +
                                                    s"shard=$shardNum ${partition.stringTsKey}")
                                                  Some(toODP)
                                                }
                                                else None
                                              } else Some(req) // if no chunks ingested yet, read everything from disk
        case InMemoryChunkScan            =>  None // Return only in-memory data - ie return none so we never ODP
        case WriteBufferChunkScan         =>  None // Write buffers are always in memory only
      }
    } else {
      None
    }
  }
}
