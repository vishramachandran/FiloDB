package filodb.core.memstore

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.StampedLock

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Random, Try}

import bloomfilter.CanGenerateHashFrom
import bloomfilter.mutable.BloomFilter
import com.googlecode.javaewah.{EWAHCompressedBitmap, IntIterator}
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import debox.{Buffer, Map => DMap}
import kamon.Kamon
import kamon.metric.{Counter, MeasurementUnit}
import kamon.tag.TagSet
import monix.eval.Task
import monix.execution.{Scheduler, UncaughtExceptionReporter}
import monix.execution.atomic.AtomicBoolean
import monix.reactive.Observable
import org.jctools.maps.NonBlockingHashMapLong
import org.jctools.queues.MpscChunkedArrayQueue
import spire.syntax.cfor._

import filodb.core.{ErrorResponse, _}
import filodb.core.binaryrecord2._
import filodb.core.memstore.ratelimit.{CardinalityRecord, CardinalityTracker, QuotaSource, RocksDbCardinalityStore}
import filodb.core.metadata.{Schema, Schemas}
import filodb.core.query.{ColumnFilter, Filter, QuerySession}
import filodb.core.store._
import filodb.memory._
import filodb.memory.data.Shutdown
import filodb.memory.format.{UnsafeUtils, ZeroCopyUTF8String}
import filodb.memory.format.BinaryVector.BinaryVectorPtr
import filodb.memory.format.ZeroCopyUTF8String._

class TimeSeriesShardStats(dataset: DatasetRef, shardNum: Int) {
  val tags = Map("shard" -> shardNum.toString, "dataset" -> dataset.toString)

  val timeseriesCount = Kamon.gauge("memstore-timeseries-count")
  val shardTotalRecoveryTime = Kamon.gauge("memstore-total-shard-recovery-time",
    MeasurementUnit.time.milliseconds).withTags(TagSet.from(tags))
  val chunksQueried = Kamon.counter("memstore-chunks-queried").withTags(TagSet.from(tags))
  val chunksQueriedByShardKey = Kamon.counter("memstore-chunks-queried-by-shardkey")
  val tsCountBySchema = Kamon.gauge("memstore-timeseries-by-schema").withTags(TagSet.from(tags))
  val rowsIngested = Kamon.counter("memstore-rows-ingested").withTags(TagSet.from(tags))
  val numTsCreated = Kamon.counter("memstore-partitions-created").withTags(TagSet.from(tags))
  val dataDropped = Kamon.counter("memstore-data-dropped").withTags(TagSet.from(tags))
  val unknownSchemaDropped = Kamon.counter("memstore-unknown-schema-dropped").withTags(TagSet.from(tags))
  val oldContainers = Kamon.counter("memstore-incompatible-containers").withTags(TagSet.from(tags))
  val offsetsNotRecovered = Kamon.counter("memstore-offsets-not-recovered").withTags(TagSet.from(tags))
  val outOfOrderDropped = Kamon.counter("memstore-out-of-order-samples").withTags(TagSet.from(tags))
  val rowsSkipped  = Kamon.counter("recovery-row-skipped").withTags(TagSet.from(tags))
  val rowsPerContainer = Kamon.histogram("num-samples-per-container").withoutTags()
  val numSamplesEncoded = Kamon.counter("memstore-samples-encoded").withTags(TagSet.from(tags))
  val encodedBytes  = Kamon.counter("memstore-encoded-bytes-allocated", MeasurementUnit.information.bytes)
    .withTags(TagSet.from(tags))
  val encodedHistBytes = Kamon.counter("memstore-hist-encoded-bytes", MeasurementUnit.information.bytes)
    .withTags(TagSet.from(tags))
  val flushesSuccessful = Kamon.counter("memstore-flushes-success").withTags(TagSet.from(tags))
  val flushesFailedTsWrite = Kamon.counter("memstore-flushes-failed-partition").withTags(TagSet.from(tags))
  val flushesFailedChunkWrite = Kamon.counter("memstore-flushes-failed-chunk").withTags(TagSet.from(tags))
  val flushesFailedOther = Kamon.counter("memstore-flushes-failed-other").withTags(TagSet.from(tags))

  val numDirtyTsKeysFlushed = Kamon.counter("memstore-index-num-dirty-keys-flushed").withTags(TagSet.from(tags))
  val indexRecoveryNumRecordsProcessed = Kamon.counter("memstore-index-recovery-partkeys-processed").
    withTags(TagSet.from(tags))
  val downsampleRecordsCreated = Kamon.counter("memstore-downsample-records-created").withTags(TagSet.from(tags))

  /**
    * These gauges are intended to be combined with one of the latest offset of Kafka timeseries so we can produce
    * stats on message lag:
    *   kafka_ingestion_lag = kafka_latest_offset - offsetLatestInMem
    *   memstore_ingested_to_persisted_lag = offsetLatestInMem - offsetLatestFlushed
    *   etc.
    *
    * NOTE: only positive offsets will be recorded.  Kafka does not give negative offsets, but Kamon cannot record
    * negative numbers either.
    * The "latest" vs "earliest" flushed reflects that there are really n offsets, one per flush group.
    */
  val offsetLatestInMem = Kamon.gauge("shard-offset-latest-inmemory").withTags(TagSet.from(tags))
  val offsetLatestFlushed = Kamon.gauge("shard-offset-flushed-latest").withTags(TagSet.from(tags))
  val offsetEarliestFlushed = Kamon.gauge("shard-offset-flushed-earliest").withTags(TagSet.from(tags))
  val numTsOnHeap = Kamon.gauge("num-partitions").withTags(TagSet.from(tags))
  val numTsActivelyIngesting = Kamon.gauge("num-ingesting-partitions").withTags(TagSet.from(tags))

  val numChunksPagedIn = Kamon.counter("chunks-paged-in").withTags(TagSet.from(tags))
  val tsPagedFromColStore = Kamon.counter("memstore-partitions-paged-in").withTags(TagSet.from(tags))
  val numTsQueried = Kamon.counter("memstore-partitions-queried").withTags(TagSet.from(tags))
  val numTsPurgedFromHeap = Kamon.counter("memstore-partitions-purged").withTags(TagSet.from(tags))
  val numTsPurgedFromIndex = Kamon.counter("memstore-partitions-purged-index").withTags(TagSet.from(tags))
  val purgeTsTimeMs = Kamon.counter("memstore-partitions-purge-time-ms", MeasurementUnit.time.milliseconds)
                                              .withTags(TagSet.from(tags))
  val numTsRestored = Kamon.counter("memstore-partitions-paged-restored").withTags(TagSet.from(tags))
  val chunkIdsEvicted = Kamon.counter("memstore-chunkids-evicted").withTags(TagSet.from(tags))
  val numTsEvicted = Kamon.counter("memstore-partitions-evicted").withTags(TagSet.from(tags))
  val queryTimeRangeMins = Kamon.histogram("query-time-range-minutes").withTags(TagSet.from(tags))
  val memoryStats = new MemoryStats(tags)

  val bufferPoolSize = Kamon.gauge("memstore-writebuffer-pool-size").withTags(TagSet.from(tags))
  val indexEntries = Kamon.gauge("memstore-index-entries").withTags(TagSet.from(tags))
  val indexBytes   = Kamon.gauge("memstore-index-ram-bytes").withTags(TagSet.from(tags))

  val evictedTsKeyBloomFilterQueries = Kamon.counter("evicted-pk-bloom-filter-queries").withTags(TagSet.from(tags))
  val evictedTsKeyBloomFilterFalsePositives = Kamon.counter("evicted-pk-bloom-filter-fp").withTags(TagSet.from(tags))
  val evictedPkBloomFilterSize = Kamon.gauge("evicted-pk-bloom-filter-approx-size").withTags(TagSet.from(tags))
  val evictedTsIdLookupMultiMatch = Kamon.counter("evicted-partId-lookup-multi-match").withTags(TagSet.from(tags))

  /**
    * Difference between the local clock and the received ingestion timestamps, in milliseconds.
    * If this gauge is negative, then the received timestamps are ahead, and it will stay this
    * way for a bit, due to the monotonic adjustment. When the gauge value is positive (which is
    * expected), then the delay reflects the delay between the generation of the samples and
    * receiving them, assuming that the clocks are in sync.
    */
  val ingestionClockDelay = Kamon.gauge("ingestion-clock-delay",
    MeasurementUnit.time.milliseconds).withTags(TagSet.from(tags))
  val chunkFlushTaskLatency = Kamon.histogram("chunk-flush-task-latency-after-retries",
    MeasurementUnit.time.milliseconds).withTags(TagSet.from(tags))

  /**
   * How much time a thread was potentially stalled while attempting to ensure
   * free space. Unit is nanoseconds.
   */
  val memstoreEvictionStall = Kamon.counter("memstore-eviction-stall",
                           MeasurementUnit.time.nanoseconds).withTags(TagSet.from(tags))
  val evictableTsKeysSize = Kamon.gauge("memstore-num-evictable-partkeys").withTags(TagSet.from(tags))

}

object TimeSeriesShard {
  /**
    * Writes metadata for TimeSeries where every vector is written
    */
  def writeMeta(addr: Long, tsId: Int, info: ChunkSetInfo, vectors: Array[BinaryVectorPtr]): Unit = {
    UnsafeUtils.setInt(UnsafeUtils.ZeroPointer, addr, tsId)
    ChunkSetInfo.copy(info, addr + 4)
    cforRange { 0 until vectors.size } { i =>
      ChunkSetInfo.setVectorPtr(addr + 4, i, vectors(i))
    }
  }

  /**
    * Copies serialized ChunkSetInfo bytes from persistent storage / on-demand paging.
    */
  def writeMeta(addr: Long, tsId: Int, bytes: Array[Byte], vectors: ArrayBuffer[BinaryVectorPtr]): Unit = {
    UnsafeUtils.setInt(UnsafeUtils.ZeroPointer, addr, tsId)
    ChunkSetInfo.copy(bytes, addr + 4)
    cforRange { 0 until vectors.size } { i =>
      ChunkSetInfo.setVectorPtr(addr + 4, i, vectors(i))
    }
  }

  /**
    * Copies serialized ChunkSetInfo bytes from persistent storage / on-demand paging.
    */
  def writeMetaWithoutTsId(addr: Long, bytes: Array[Byte], vectors: Array[BinaryVectorPtr]): Unit = {
    ChunkSetInfo.copy(bytes, addr)
    cforRange { 0 until vectors.size } { i =>
      ChunkSetInfo.setVectorPtr(addr, i, vectors(i))
    }
  }

  // Initial size of tsSet and timeseries map structures.  Make large enough to avoid too many resizes.
  val InitialNumTs = 128 * 1024

  // Not a real timeseries, just a special marker for "out of memory"
  val OutOfMemTs = UnsafeUtils.ZeroPointer.asInstanceOf[TimeSeries]

  val EmptyBitmap = new EWAHCompressedBitmap()

  /**
    * Calculates the flush group of an ingest record or timeseries key.  Be sure to use the right RecordSchema -
    * dataset.ingestionSchema or dataset.tsKeySchema.l
    */
  def tsKeyGroup(schema: RecordSchema, tsKeyBase: Any, tsKeyOffset: Long, numGroups: Int): Int = {
    Math.abs(schema.tsHash(tsKeyBase, tsKeyOffset) % numGroups)
  }

  private[memstore] final val CREATE_NEW_TS_ID = -1
}

private[core] final case class TsKey(base: Any, offset: Long)
private[core] final case class TsKeyWithTimes(base: Any, offset: Long, startTime: Long, endTime: Long)

trait TimeSeriesIterator extends Iterator[TimeSeries] {
  def skippedTsIds: Buffer[Int]
}

object TimeSeriesIterator {
  def fromTsIt(baseIt: Iterator[TimeSeries]): TimeSeriesIterator = new TimeSeriesIterator {
    val skippedTsIds = Buffer.empty[Int]
    final def hasNext: Boolean = baseIt.hasNext
    final def next: TimeSeries = baseIt.next
  }
}

/**
  * TimeSeries lookup from filters result, usually step 1 of querying.
  *
  * @param tsInMemory iterates through the in-Memory timeseries, some of which may not need ODP.
  *                          Caller needs to filter further
  * @param firstSchemaId if defined, the first Schema ID found. If not defined, probably there's no data.
  * @param tsIdsMemTimeGap contains tsIds in memory but with potential time gaps in data. Their
  *                          startTimes from Lucene are mapped from the ID.
  * @param tsIdsNotInMemory is a collection of tsIds fully not in memory
  */
case class TsLookupResult(shard: Int,
                          chunkMethod: ChunkScanMethod,
                          tsInMemory: debox.Buffer[Int],
                          firstSchemaId: Option[Int] = None,
                          tsIdsMemTimeGap: debox.Map[Int, Long] = debox.Map.empty,
                          tsIdsNotInMemory: debox.Buffer[Int] = debox.Buffer.empty,
                          tsKeyRecords: Seq[TsKeyLuceneIndexRecord] = Seq.empty,
                          queriedChunksCounter: Counter)

final case class SchemaMismatch(expected: String, found: String) extends
Exception(s"Multiple schemas found, please filter. Expected schema $expected, found schema $found")

object SchemaMismatch {
  def apply(expected: Schema, found: Schema): SchemaMismatch = SchemaMismatch(expected.name, found.name)
}

// scalastyle:off number.of.methods
// scalastyle:off file.size.limit
/**
  * Contains all of the data for a SINGLE shard of a time series oriented dataset.
  *
  * Each timeseries has an integer ID which is used for bitmap indexing using the lucene index.
  * Within a shard, the timeseries are grouped into a fixed number of groups to facilitate persistence and recovery:
  * - groups spread out the persistence/flushing load across time
  * - having smaller batches of flushes shortens the window of recovery and enables skipping of records/less CPU
  *
  * Each incoming time series is hashed into a group.  Each group has its own watermark.  The watermark indicates,
  * for that group, up to what offset incoming records for that group has been persisted.  At recovery time, records
  * that fall below the watermark for that group will be skipped (since they can be recovered from disk).
  *
  * @param bufferMemoryManager Unencoded/unoptimized ingested data is stored in buffers that are allocated from this
  *                            memory pool. This pool is also used to store timeseries keys.
  * @param storeConfig the store portion of the sourceconfig, not the global FiloDB application config
  */
class TimeSeriesShard(val ref: DatasetRef,
                      val schemas: Schemas,
                      val storeConfig: StoreConfig,
                      quotaSource: QuotaSource,
                      val shardNum: Int,
                      val bufferMemoryManager: NativeMemoryManager,
                      colStore: ColumnStore,
                      metastore: MetaStore,
                      evictionPolicy: TimeSeriesEvictionPolicy,
                      filodbConfig: Config)
                     (implicit val ioPool: ExecutionContext) extends StrictLogging {
  import collection.JavaConverters._

  import FiloSchedulers._
  import TimeSeriesShard._

  @volatile var isReadyForQuery = false

  val shardStats = new TimeSeriesShardStats(ref, shardNum)
  val shardKeyLevelIngestionMetricsEnabled = filodbConfig.getBoolean("shard-key-level-ingestion-metrics-enabled")
  val shardKeyLevelQueryMetricsEnabled = filodbConfig.getBoolean("shard-key-level-query-metrics-enabled")

  val creationTime = System.currentTimeMillis()

  /**
    * Map of all timeseries in the shard stored in memory, indexed by timeseries ID
    */
  private[memstore] val tsIdToTsMap = new NonBlockingHashMapLong[TimeSeries](InitialNumTs, false)

  /**
    * next time series ID number
    */
  private var nextTsId = 0

  /**
    * This index helps identify which timeseries have any given column-value.
    * Used to answer queries not involving the full timeseries key.
    * Maintained using a high-performance bitmap index.
    */
  private[memstore] final val tsKeyTagValueIndex = new TsKeyLuceneIndex(ref, schemas.ts, shardNum,
    storeConfig.diskTTLSeconds * 1000)

  private val cardTracker: CardinalityTracker = if (storeConfig.meteringEnabled) {
    val cardStore = new RocksDbCardinalityStore(ref, shardNum)
    val defaultQuota = quotaSource.getDefaults(ref)
    val tracker = new CardinalityTracker(ref, shardNum, schemas.ts.options.shardKeyColumns.length,
                                                     defaultQuota, cardStore)
    quotaSource.getQuotas(ref).foreach { q =>
      tracker.setQuota(q.shardKeyPrefix, q.quota)
    }
    tracker
  } else UnsafeUtils.ZeroPointer.asInstanceOf[CardinalityTracker]

  /**
    * Keeps track of count of rows ingested into memstore, not necessarily flushed.
    * This is generally used to report status and metrics.
    */
  private final var ingested = 0L

  private val maxTimeSeriesCount = filodbConfig.getInt("memstore.max-partitions-on-heap-per-shard")
  private val ensureTspHeadroomPercent = filodbConfig.getDouble("memstore.ensure-tsp-count-headroom-percent")
  private val ensureBlockHeadroomPercent = filodbConfig.getDouble("memstore.ensure-block-memory-headroom-percent")
  private val ensureNativeMemHeadroomPercent = filodbConfig.getDouble("memstore.ensure-native-memory-headroom-percent")

  /**
   * Queue of tsIds that are eligible for eviction since they have stopped ingesting.
   * Caller needs to double check ingesting status since they may have started to re-ingest
   * since tsId was added to this queue.
   * Mpsc since the producers are flush task and odp timeseries creation task
   * FIXME we can create a more efficient data structure that stores the ints in unboxed form - uses less heap
   */
  protected[memstore] final val evictableTsIds = new MpscChunkedArrayQueue[Int](1024, maxTimeSeriesCount)
  protected[memstore] final val evictableOdpTsIds = new MpscChunkedArrayQueue[Int](128, maxTimeSeriesCount)

  /**
    * Keeps track of last offset ingested into memory (not necessarily flushed).
    * This value is used to keep track of the checkpoint to be written for next flush for any group.
    */
  private final var _offset = Long.MinValue

  /**
   * The maximum blockMetaSize amongst all the schemas this Dataset could ingest
   */
  val maxMetaSize = schemas.schemas.values.map(_.data.blockMetaSize).max

  require (storeConfig.maxChunkTime > storeConfig.flushInterval, "MaxChunkTime should be greater than FlushInterval")
  val maxChunkTime = storeConfig.maxChunkTime.toMillis

  val acceptDuplicateSamples = storeConfig.acceptDuplicateSamples

  // Called to remove chunks from ChunkMap of a given timeseries, when an offheap block is reclaimed
  private val reclaimListener = new ReclaimListener {
    def onReclaim(metaAddr: Long, numBytes: Int): Unit = {
      val tsId = UnsafeUtils.getInt(metaAddr)
      val ts = tsIdToTsMap.get(tsId)
      if (ts != UnsafeUtils.ZeroPointer) {
        // The number of bytes passed in is the metadata size which depends on schema.  It should match the
        // timeseries's blockMetaSize; if it doesn't that is a flag for possible corruption, and we should halt
        // the process to be safe and log details for further debugging.
        val chunkID = UnsafeUtils.getLong(metaAddr + 4)
        if (numBytes != ts.schema.data.blockMetaSize) {
          Shutdown.haltAndCatchFire( new RuntimeException(f"POSSIBLE CORRUPTION DURING onReclaim(" +
                       f"metaAddr=0x$metaAddr%08x, numBytes=$numBytes)" +
                       s"Expected meta size: ${ts.schema.data.blockMetaSize} for schema=${ts.schema} " +
                       s"Reclaiming chunk chunkID=$chunkID from shard=$shardNum tsId=$tsId ${ts.stringTsKey}"))
        }
        ts.removeChunksAt(chunkID)
        logger.debug(s"Reclaiming chunk chunkID=$chunkID from shard=$shardNum " +
          s"tsID=$tsId ${ts.stringTsKey}")
      }
    }
  }

  // Create a single-threaded scheduler just for ingestion.  Name the thread for ease of debugging
  // NOTE: to control intermixing of different Observables/Tasks in this thread, customize ExecutionModel param
  val ingestSched = Scheduler.singleThread(s"$IngestSchedName-$ref-$shardNum",
    reporter = UncaughtExceptionReporter(logger.error("Uncaught Exception in TimeSeriesShard.ingestSched", _)))

  private val blockMemorySize = storeConfig.shardMemSize
  protected val numGroups = storeConfig.groupsPerShard
  private val chunkRetentionHours = (storeConfig.diskTTLSeconds / 3600).toInt
  val pagingEnabled = storeConfig.demandPagingEnabled

  /**
    * Access TimeSeries using ingest record timeseries key in O(1) time.
    */
  private[memstore] final val tsKeyToTs = TimeSeriesSet.ofSize(InitialNumTs)
  // Use a StampedLock because it supports optimistic read locking. This means that no blocking
  // occurs in the common case, when there isn't any contention reading from TimeseriesSet.
  private[memstore] final val tsSetLock = new StampedLock

  /**
   * Lock that protects chunks and TSPs from being reclaimed from Memstore.
   * This is needed to prevent races between ODP queries and reclaims and ensure that
   * TSPs and chunks dont get evicted when queries are being served.
   */
  private[memstore] final val evictionLock = new EvictionLock(s"shard=$shardNum dataset=$ref")

  // The off-heap block store used for encoded chunks
  private val shardTags = Map("dataset" -> ref.dataset, "shard" -> shardNum.toString)
  private val blockStore = new PageAlignedBlockManager(blockMemorySize, shardStats.memoryStats, reclaimListener,
    storeConfig.numPagesPerBlock, evictionLock)
  private[core] val blockFactoryPool = new BlockMemFactoryPool(blockStore, maxMetaSize, shardTags)

  // Requires blockStore.
  private val headroomTask = startHeadroomTask(ingestSched)

  val odpChunkStore = new DemandPagedChunkStore(this, blockStore)

  private val tsKeyBuilder = new RecordBuilder(MemFactory.onHeapFactory, reuseOneContainer = true)
  private val tsKeyArray = tsKeyBuilder.allContainers.head.base.asInstanceOf[Array[Byte]]
  private[memstore] val bufferPools = {
    val pools = schemas.schemas.values.map { sch =>
      sch.schemaHash -> new WriteBufferPool(bufferMemoryManager, sch.data, storeConfig)
    }
    DMap(pools.toSeq: _*)
  }

  private final val tsGroups = Array.fill(numGroups)(new EWAHCompressedBitmap)

  /**
    * Bitmap to track actively ingesting timeseries.
    * This bitmap is maintained in addition to the ingesting flag per timeseries.
    * TSP.ingesting is MUCH faster than bit.get(i) but we need the bitmap for faster operations
    * for all timeseries of shard (like ingesting cardinality counting, rollover of time buckets etc).
    */
  private[memstore] final val activelyIngesting = debox.Set.empty[Int]

  private val numFlushIntervalsDuringRetention = Math.ceil(chunkRetentionHours.hours / storeConfig.flushInterval).toInt

  // Use 1/4 of flush intervals within retention period for initial ChunkMap size
  private val initInfoMapSize = Math.max((numFlushIntervalsDuringRetention / 4) + 4, 20)

  /**
    * Dirty timeseries whose start/end times have not been updated to cassandra.
    *
    * IMPORTANT. Only modify this var in IngestScheduler
    */
  private[memstore] final var dirtyTimeSeriesForIndexFlush = debox.Buffer.empty[Int]

  /**
    * This is the group during which this shard will flush dirty timeseries keys. Randomized to
    * ensure we dont flush time buckets across shards at same time
    */
  private final val dirtyTsKeysFlushGroup = Random.nextInt(numGroups)
  logger.info(s"Dirty timeseries Keys for shard=$shardNum will flush in group $dirtyTsKeysFlushGroup")

  /**
    * The offset up to and including the last record in this group to be successfully persisted.
    * Also used during recovery to figure out what incoming records to skip (since it's persisted)
    */
  private final val groupWatermark = Array.fill(numGroups)(Long.MinValue)

  /**
    * Highest ingestion timestamp observed.
    */
  private[memstore] var lastIngestionTime = Long.MinValue

  // Flush groups when ingestion time is observed to cross a time boundary (typically an hour),
  // plus a group-specific offset. This simplifies disaster recovery -- chunks can be copied
  // without concern that they may overlap in time.
  private val flushBoundaryMillis = Option(storeConfig.flushInterval.toMillis)

  // Defines the group-specific flush offset, to distribute the flushes around such they don't
  // all flush at the same time. With an hourly boundary and 60 flush groups, flushes are
  // scheduled once a minute.
  private val flushOffsetMillis = flushBoundaryMillis.get / numGroups

  private[memstore] val evictedTsKeysBF =
    BloomFilter[TsKey](storeConfig.evictedPkBfCapacity, falsePositiveRate = 0.01)(new CanGenerateHashFrom[TsKey] {
      override def generateHash(from: TsKey): Long = {
        schemas.ts.binSchema.tsHash(from.base, from.offset)
      }
    })
  private var evictedTsKeysBFDisposed = false

  private val brRowReader = new MultiSchemaBRRowReader()

  /**
    * Detailed filtered ingestion record logging.  See "trace-filters" StoreConfig setting.  Warning: may blow up
    * logs, use at your own risk.
    */
  val tracedTsFilters = storeConfig.traceFilters

  /**
    * Iterate TimeSeries objects relevant to given tsIds.
    */
  case class InMemTimeSeriesIterator(intIt: IntIterator) extends TimeSeriesIterator {
    var nextTs = UnsafeUtils.ZeroPointer.asInstanceOf[TimeSeries]
    val skippedTsIds = debox.Buffer.empty[Int]
    private def findNext(): Unit = {
      while (intIt.hasNext && nextTs == UnsafeUtils.ZeroPointer) {
        val nextTsId = intIt.next
        nextTs = tsIdToTsMap.get(nextTsId)
        if (nextTs == UnsafeUtils.ZeroPointer) skippedTsIds += nextTsId
      }
    }

    findNext()

    final def hasNext: Boolean = nextTs != UnsafeUtils.ZeroPointer
    final def next: TimeSeries = {
      val toReturn = nextTs
      nextTs = UnsafeUtils.ZeroPointer.asInstanceOf[TimeSeries] // reset so that we can keep going
      findNext()
      toReturn
    }
  }

  /**
    * Iterate TimeSeries objects relevant to given tsIds.
    */
  case class InMemTimeSeriesIterator2(tsIds: debox.Buffer[Int]) extends TimeSeriesIterator {
    var nextTs = UnsafeUtils.ZeroPointer.asInstanceOf[TimeSeries]
    val skippedTsIds = debox.Buffer.empty[Int]
    var nextTsId = -1
    findNext()

    private def findNext(): Unit = {
      while (nextTsId + 1 < tsIds.length && nextTs == UnsafeUtils.ZeroPointer) {
        nextTsId += 1
        nextTs = tsIdToTsMap.get(tsIds(nextTsId))
        if (nextTs == UnsafeUtils.ZeroPointer) skippedTsIds += tsIds(nextTsId)
      }
    }

    final def hasNext: Boolean = nextTs != UnsafeUtils.ZeroPointer

    final def next: TimeSeries = {
      val toReturn = nextTs
      nextTs = UnsafeUtils.ZeroPointer.asInstanceOf[TimeSeries] // reset so that we can keep going
      findNext()
      toReturn
    }
  }

  // RECOVERY: Check the watermark for the group that this record is part of.  If the ingestOffset is < watermark,
  // then do not bother with the expensive timeseries key comparison and ingestion.  Just skip it
  class IngestConsumer(var ingestionTime: Long = 0,
                       var numActuallyIngested: Int = 0,
                       var ingestOffset: Long = -1L) extends BinaryRegionConsumer {
    // Receives a new ingestion BinaryRecord
    final def onNext(recBase: Any, recOffset: Long): Unit = {
      val schemaId = RecordSchema.schemaID(recBase, recOffset)
      val schema = schemas(schemaId)
      if (schema != Schemas.UnknownSchema) {
        val group = tsKeyGroup(schema.ingestionSchema, recBase, recOffset, numGroups)
        if (ingestOffset < groupWatermark(group)) {
          shardStats.rowsSkipped.increment()
          try {
            // Needed to update index with new timeseries added during recovery with correct startTime.
            // This is important to do since the group designated for dirty timeseries key persistence can
            // lag behind group the timeseries belongs to. Hence during recovery, we skip
            // ingesting the sample, but create the timeseries and mark it as dirty.
            // TODO:
            // explore aligning index time buckets with chunks, and we can then
            // remove this timeseries existence check per sample.
            val ts: FiloTimeSeries = getOrAddTimeSeriesForIngestion(recBase, recOffset, group, schema)
            if (ts == OutOfMemTs) { disableAddTimeSeries() }
          } catch {
            case e: OutOfOffheapMemoryException => disableAddTimeSeries()
            case e: Exception                   => logger.error(s"Unexpected ingestion err", e); disableAddTimeSeries()
          }
        } else {
          getOrAddTimeSeriesAndIngest(ingestionTime, recBase, recOffset, group, schema)
          numActuallyIngested += 1
        }
      } else {
        logger.debug(s"Unknown schema ID $schemaId will be ignored during ingestion")
        shardStats.unknownSchemaDropped.increment()
      }
    }
  }

  private[memstore] val ingestConsumer = new IngestConsumer()

  /**
    * Ingest new BinaryRecords in a RecordContainer to this shard.
    * Skips rows if the offset is below the group watermark for that record's group.
    * Adds new timeseries if needed.
    */
  def ingest(container: RecordContainer, offset: Long): Long = {
    assertThreadName(IngestSchedName)
    if (container.isCurrentVersion) {
      if (!container.isEmpty) {
        ingestConsumer.ingestionTime = container.timestamp
        ingestConsumer.numActuallyIngested = 0
        ingestConsumer.ingestOffset = offset
        brRowReader.recordBase = container.base
        container.consumeRecords(ingestConsumer)
        shardStats.rowsIngested.increment(ingestConsumer.numActuallyIngested)
        shardStats.rowsPerContainer.record(ingestConsumer.numActuallyIngested)
        ingested += ingestConsumer.numActuallyIngested
        _offset = offset
      }
    } else {
      shardStats.oldContainers.increment()
    }
    _offset
  }

  def topKCardinality(k: Int, shardKeyPrefix: Seq[String]): Seq[CardinalityRecord] = {
    if (storeConfig.meteringEnabled) cardTracker.topk(k, shardKeyPrefix)
    else throw new IllegalArgumentException("Metering is not enabled")
  }

  def startFlushingIndex(): Unit =
    tsKeyTagValueIndex.startFlushThread(storeConfig.tsIndexFlushMinDelaySeconds,
                                        storeConfig.tsIndexFlushMaxDelaySeconds)

  def ingest(data: SomeData): Long = ingest(data.records, data.offset)

  def recoverIndex(): Future[Unit] = {
    val indexBootstrapper = new IndexBootstrapper(colStore)
    indexBootstrapper.bootstrapIndexRaw(tsKeyTagValueIndex, shardNum, ref)(bootstrapTsKey)
                     .executeOn(ingestSched) // to make sure bootstrapIndex task is run on ingestion thread
                     .map { count =>
                        startFlushingIndex()
                       logger.info(s"Bootstrapped index for dataset=$ref shard=$shardNum with $count records")
                     }.runAsync(ingestSched)
  }

  /**
    * Handles actions to be performed for the shard upon bootstrapping
    * a timeseries key from index store
    * @param tsRec tsKey Record
    * @return tsId assigned to key
    */
  // scalastyle:off method.length
  private[memstore] def bootstrapTsKey(tsRec: TsKeyRecord): Int = {
    assertThreadName(IngestSchedName)
    val schemaId = RecordSchema.schemaID(tsRec.tsKey, UnsafeUtils.arayOffset)
    val schema = schemas(schemaId)
    val tsId = if (tsRec.endTime == Long.MaxValue) {
      // this is an actively ingesting timeseries
      val group = tsKeyGroup(schemas.ts.binSchema, tsRec.tsKey, UnsafeUtils.arayOffset, numGroups)
      if (schema != Schemas.UnknownSchema) {
        val ts = createNewTimeSeries(tsRec.tsKey, UnsafeUtils.arayOffset, group, CREATE_NEW_TS_ID, schema, false, 4)
        // In theory, we should not get an OutOfMemTimeSeries here since
        // it should have occurred before node failed too, and with data stopped,
        // index would not be updated. But if for some reason we see it, drop data
        if (ts == OutOfMemTs) {
          logger.error("Could not accommodate tsKey while recovering index. " +
            "WriteBuffer size may not be configured correctly")
          -1
        } else {
          val stamp = tsSetLock.writeLock()
          try {
            tsKeyToTs.add(ts) // createNewTimeSeries doesn't add ts to timeseriesSet
            ts.ingesting = true
            ts.tsId
          } finally {
            tsSetLock.unlockWrite(stamp)
          }
        }
      } else {
        logger.info(s"Ignoring ts key with unknown schema ID $schemaId")
        shardStats.unknownSchemaDropped.increment()
        -1
      }
    } else {
      // assign a new tsId to non-ingesting timeseries,
      // but no need to create a new TimeSeries heap object
      // instead add the timeseries to evictedTsKeysBF bloom filter so that it can be found if necessary
      evictedTsKeysBF.synchronized {
        require(!evictedTsKeysBFDisposed)
        evictedTsKeysBF.add(TsKey(tsRec.tsKey, UnsafeUtils.arayOffset))
      }
      createTsId()
    }

    activelyIngesting.synchronized {
      if (tsRec.endTime == Long.MaxValue) activelyIngesting += tsId
      else activelyIngesting -= tsId
    }
    shardStats.indexRecoveryNumRecordsProcessed.increment()
    if (schema != Schemas.UnknownSchema) {
      val shardKey = schema.tsKeySchema.colValues(tsRec.tsKey, UnsafeUtils.arayOffset, schema.options.shardKeyColumns)
      captureTimeseriesCount(schema, shardKey, 1)
      if (storeConfig.meteringEnabled) {
        cardTracker.incrementCount(shardKey)
      }
    }
    tsId
  }

  private def captureTimeseriesCount(schema: Schema, shardKey: Seq[String], times: Double) = {
    // Assuming that the last element in the shardKeyColumn is always a metric name, we are making sure the
    // shardKeyColumn.length is > 1 and dropping the last element in shardKeyColumn.
    if (shardKeyLevelIngestionMetricsEnabled &&
        schema.options.shardKeyColumns.length > 1 &&
        shardKey.length == schema.options.shardKeyColumns.length) {
      val tagSetMap = (schema.options.shardKeyColumns.map(c => s"metric${c}tag") zip shardKey).dropRight(1).toMap
      shardStats.timeseriesCount.withTags(TagSet.from(tagSetMap)).increment(times)
    }
    shardStats.tsCountBySchema.withTag("schema", schema.name).increment(times)
  }

  def indexNames(limit: Int): Seq[String] = tsKeyTagValueIndex.indexNames(limit)

  def labelValues(labelName: String, topK: Int): Seq[TermInfo] = tsKeyTagValueIndex.indexValues(labelName, topK)

  /**
    * This method is to apply column filters and fetch matching time series timeseries.
    *
    * @param filter column filter
    * @param labelNames labels to return in the response
    * @param endTime end time
    * @param startTime start time
    * @param limit series limit
    * @return returns an iterator of map of label key value pairs of each matching time series
    */
  def labelValuesWithFilters(filter: Seq[ColumnFilter],
                             labelNames: Seq[String],
                             endTime: Long,
                             startTime: Long,
                             limit: Int): Iterator[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]] = {
    LabelValueResultIterator(tsKeyTagValueIndex.tsIdsFromFilters(filter, startTime, endTime), labelNames, limit)
  }

  /**
   * Iterator for traversal of tsIds, value for the given label will be extracted from the ParitionKey.
   * this implementation maps tsIds to label/values eagerly, this is done inorder to dedup the results.
   */
  case class LabelValueResultIterator(tsIds: debox.Buffer[Int], labelNames: Seq[String], limit: Int)
    extends Iterator[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]] {
    private lazy val rows = labelValues

    def labelValues: Iterator[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]] = {
      var tsLoopIndx = 0
      val rows = new mutable.HashSet[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]]()
      while(tsLoopIndx < tsIds.length && rows.size < limit) {
        val tsId = tsIds(tsLoopIndx)

        //retrieve tsKey either from In-memory map or from TsKeyIndex
        val nextTs = tsKeyFromTsId(tsId)

        // FIXME This is non-performant and temporary fix for fetching label values based on filter criteria.
        // Other strategies needs to be evaluated for making this performant - create facets for predefined fields or
        // have a centralized service/store for serving metadata

        val currVal = schemas.ts.binSchema.colValues(nextTs.base, nextTs.offset, labelNames).
          zipWithIndex.filter(_._1 != null).map{case(value, ind) => labelNames(ind).utf8 -> value.utf8}.toMap

        if (currVal.nonEmpty) rows.add(currVal)
        tsLoopIndx += 1
      }
      rows.toIterator
    }

    override def hasNext: Boolean = rows.hasNext

    override def next(): Map[ZeroCopyUTF8String, ZeroCopyUTF8String] = rows.next
  }

  /**
    * This method is to apply column filters and fetch matching time series timeseries keys.
    */
  def tsKeysWithFilters(filter: Seq[ColumnFilter],
                        fetchFirstLastSampleTimes: Boolean,
                        endTime: Long,
                        startTime: Long,
                        limit: Int): Iterator[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]] = {
    if (fetchFirstLastSampleTimes) {
      tsKeyTagValueIndex.tsKeyRecordsFromFilters(filter, startTime, endTime).iterator.map { pk =>
        val tsKeyMap = convertTsKeyWithTimesToMap(
          TsKeyWithTimes(pk.tsKey, UnsafeUtils.arayOffset, pk.startTime, pk.endTime))
        tsKeyMap ++ Map(
          ("_firstSampleTime_".utf8, pk.startTime.toString.utf8),
          ("_lastSampleTime_".utf8, pk.endTime.toString.utf8))
      } take(limit)
    } else {
      val tsIds = tsKeyTagValueIndex.tsIdsFromFilters(filter, startTime, endTime)
      val inMem = InMemTimeSeriesIterator2(tsIds)
      val inMemTsKeys = inMem.map { p =>
        convertTsKeyWithTimesToMap(TsKeyWithTimes(p.tsKeyBase, p.tsKeyOffset, -1, -1))}
      val skippedTsKeys = inMem.skippedTsIds.iterator().map(tsId => {
        convertTsKeyWithTimesToMap(tsKeyFromTsId(tsId))})
      (inMemTsKeys ++ skippedTsKeys).take(limit)
    }
  }

  private def convertTsKeyWithTimesToMap(tsKey: TsKeyWithTimes): Map[ZeroCopyUTF8String, ZeroCopyUTF8String] = {
    schemas.ts.binSchema.toStringPairs(tsKey.base, tsKey.offset).map(pair => {
      pair._1.utf8 -> pair._2.utf8
    }).toMap ++
      Map("_type_".utf8 -> Schemas.global.schemaName(RecordSchema.schemaID(tsKey.base, tsKey.offset)).utf8)
  }

  /**
    * retrieve tsKey for a given tsId
    */
  private def tsKeyFromTsId(tsId: Int): TsKeyWithTimes = {
    val nextTs = tsIdToTsMap.get(tsId)
    if (nextTs != UnsafeUtils.ZeroPointer)
      TsKeyWithTimes(nextTs.tsKeyBase, nextTs.tsKeyOffset, -1, -1)
    else { //retrieving TsKey from lucene index
      val tsKeyByteBuf = tsKeyTagValueIndex.tsKeyFromTsId(tsId)
      if (tsKeyByteBuf.isDefined) TsKeyWithTimes(tsKeyByteBuf.get.bytes, UnsafeUtils.arayOffset, -1, -1)
      else throw new IllegalStateException("This is not an expected behavior." +
        " tsId should always have a corresponding TsKey!")
    }
  }

  /**
    * WARNING: Not performant. Use only in tests, or during initial bootstrap.
    */
  def refreshTsKeyIndexBlocking(): Unit = tsKeyTagValueIndex.refreshReadersBlocking()

  def numRowsIngested: Long = ingested

  def numActiveTimeSeries: Int = tsKeyToTs.size

  def latestOffset: Long = _offset

  /**
    * Sets the watermark for each subgroup.  If an ingested record offset is below this watermark then it will be
    * assumed to already have been persisted, and the record will be discarded.  Use only for recovery.
    * @param watermarks a Map from group number to watermark
    */
  def setGroupWatermarks(watermarks: Map[Int, Long]): Unit =
    watermarks.foreach { case (group, mark) => groupWatermark(group) = mark }

  /**
    * Prepares the given group for flushing.  This MUST be done in the same thread/stream as
    * input records to avoid concurrency issues, and to ensure that all the timeseries in a
    * group are switched at the same watermark. Also required because this method removes
    * entries from the timeseries data structures.
    */
  def prepareFlushGroup(groupNum: Int): FlushGroup = {
    assertThreadName(IngestSchedName)

    // Rapidly switch all of the input buffers for a particular group
    logger.debug(s"Switching write buffers for group $groupNum in dataset=$ref shard=$shardNum")
    InMemTimeSeriesIterator(tsGroups(groupNum).intIterator)
      .foreach(_.switchBuffers(blockFactoryPool.checkoutForOverflow(groupNum)))

    val dirtyTsKeys = if (groupNum == dirtyTsKeysFlushGroup) {
      logger.debug(s"Switching dirty ts keys in dataset=$ref shard=$shardNum out for flush. ")
      purgeExpiredTimeSeries()
      val old = dirtyTimeSeriesForIndexFlush
      dirtyTimeSeriesForIndexFlush = debox.Buffer.empty[Int]
      old
    } else {
      debox.Buffer.ofSize[Int](0)
    }

    FlushGroup(shardNum, groupNum, latestOffset, dirtyTsKeys)
  }

  private def purgeExpiredTimeSeries(): Unit = ingestSched.executeTrampolined { () =>
    assertThreadName(IngestSchedName)
    // TODO Much of the purging work other of removing TSP from shard data structures can be done
    // asynchronously on another thread. No need to block ingestion thread for this.
    val start = System.currentTimeMillis()
    val tsIdsToPurge = tsKeyTagValueIndex.tsIdsEndedBefore(start - storeConfig.diskTTLSeconds * 1000)
    val removedTsIds = debox.Buffer.empty[Int]
    val tsIter = InMemTimeSeriesIterator2(tsIdsToPurge)
    tsIter.foreach { p =>
      if (!p.ingesting) {
        logger.debug(s"Purging timeseries with tsId=${p.tsId}  ${p.stringTsKey} from " +
          s"memory in dataset=$ref shard=$shardNum")
        val schema = p.schema
        val shardKey = schema.tsKeySchema.colValues(p.tsKeyBase, p.tsKeyOffset, schema.options.shardKeyColumns)
        captureTimeseriesCount(schema, shardKey, -1)
        if (storeConfig.meteringEnabled) {
          cardTracker.decrementCount(shardKey)
        }
        removeTimeseries(p)
        removedTsIds += p.tsId
      }
    }
    tsIter.skippedTsIds.foreach { pId =>
      tsKeyTagValueIndex.tsKeyFromTsId(pId).foreach { pk =>
        val unsafePkOffset = TsKeyLuceneIndex.bytesRefToUnsafeOffset(pk.offset)
        val schema = schemas(RecordSchema.schemaID(pk.bytes, unsafePkOffset))
        val shardKey = schema.tsKeySchema.colValues(pk.bytes, unsafePkOffset,
          schemas.ts.options.shardKeyColumns)
        if (storeConfig.meteringEnabled) {
          cardTracker.decrementCount(shardKey)
        }
        captureTimeseriesCount(schema, shardKey, -1)
      }
    }
    tsKeyTagValueIndex.removeTsKeys(tsIter.skippedTsIds)
    tsKeyTagValueIndex.removeTsKeys(removedTsIds)
    if (removedTsIds.length + tsIter.skippedTsIds.length > 0)
      logger.info(s"Purged ${removedTsIds.length} timeseries from memory/index " +
        s"and ${tsIter.skippedTsIds.length} from index only from dataset=$ref shard=$shardNum")
    shardStats.numTsPurgedFromHeap.increment(removedTsIds.length)
    shardStats.numTsPurgedFromIndex.increment(removedTsIds.length + tsIter.skippedTsIds.length)
    shardStats.purgeTsTimeMs.increment(System.currentTimeMillis() - start)
  }

  /**
    * Creates zero or more flush tasks (up to the number of flush groups) based on examination
    * of the record container's ingestion time. This should be called before ingesting the container.
    *
    * Note that the tasks returned by this method aren't executed yet. The caller decides how
    * to run the tasks, and by which threads.
    */
  def createFlushTasks(container: RecordContainer): Seq[Task[Response]] = {
    val tasks = new ArrayBuffer[Task[Response]]()

    var oldTimestamp = lastIngestionTime
    val ingestionTime = Math.max(oldTimestamp, container.timestamp) // monotonic clock
    var newTimestamp = ingestionTime

    if (newTimestamp > oldTimestamp && oldTimestamp != Long.MinValue) {
      cforRange ( 0 until numGroups ) { group =>
        /* Logically, the task creation filter is as follows:

           // Compute the time offset relative to the group number. 0 min, 1 min, 2 min, etc.
           val timeOffset = group * flushOffsetMillis

           // Adjust the timestamp relative to the offset such that the
           // division rounds correctly.
           val oldTimestampAdjusted = oldTimestamp - timeOffset
           val newTimestampAdjusted = newTimestamp - timeOffset

           if (oldTimstampAdjusted / flushBoundary != newTimestampAdjusted / flushBoundary) {
             ...

           As written the code the same thing but with fewer operations. It's also a bit
           shorter, but you also had to read this comment...
         */
        if (oldTimestamp / flushBoundaryMillis.get != newTimestamp / flushBoundaryMillis.get) {
          // Flush out the group before ingesting records for a new hour (by group offset).
          tasks += createFlushTask(prepareFlushGroup(group))
        }
        oldTimestamp -= flushOffsetMillis
        newTimestamp -= flushOffsetMillis
      }
    }

    // Only update stuff if no exception was thrown.

    if (ingestionTime != lastIngestionTime) {
        lastIngestionTime = ingestionTime
        shardStats.ingestionClockDelay.update(System.currentTimeMillis() - ingestionTime)
    }

    tasks
  }

  private def createFlushTask(flushGroup: FlushGroup): Task[Response] = {
    assertThreadName(IngestSchedName)
    // clone the bitmap so that reads on the flush thread do not conflict with writes on ingestion thread
    val tsIt = InMemTimeSeriesIterator(tsGroups(flushGroup.groupNum).clone().intIterator)
    doFlushSteps(flushGroup, tsIt)
  }

  private def updateGauges(): Unit = {
    assertThreadName(IngestSchedName)
    shardStats.bufferPoolSize.update(bufferPools.valuesArray.map(_.poolSize).sum)
    shardStats.indexEntries.update(tsKeyTagValueIndex.indexNumEntries)
    shardStats.indexBytes.update(tsKeyTagValueIndex.indexRamBytes)
    shardStats.numTsOnHeap.update(numActiveTimeSeries)
    val numIngesting = activelyIngesting.synchronized { activelyIngesting.size }
    shardStats.numTsActivelyIngesting.update(numIngesting)

    // Also publish MemFactory stats. Instance is expected to be shared, but no harm in
    // publishing a little more often than necessary.
    bufferMemoryManager.updateStats()
  }

  private def toTsKeyRecord(p: TimeSeries): TsKeyRecord = {
    assertThreadName(IOSchedName)
    var startTime = tsKeyTagValueIndex.startTimeFromTsId(p.tsId)
    if (startTime == -1) startTime = p.earliestTime // can remotely happen since lucene reads are eventually consistent
    if (startTime == Long.MaxValue) startTime = 0 // if for any reason we cant find the startTime, use 0
    val endTime = if (p.ingesting) {
      Long.MaxValue
    } else {
      val et = p.timestampOfLatestSample  // -1 can be returned if no sample after reboot
      if (et == -1) System.currentTimeMillis() else et
    }
    TsKeyRecord(p.tsKeyBytes, startTime, endTime, Some(p.tsKeyHash))
  }

  // scalastyle:off method.length
  private def doFlushSteps(flushGroup: FlushGroup,
                           tsIt: Iterator[TimeSeries]): Task[Response] = {
    assertThreadName(IngestSchedName)
    val flushStart = System.currentTimeMillis()

    // Only allocate the blockHolder when we actually have chunks/timeseries to flush
    val blockHolder = blockFactoryPool.checkoutForFlush(flushGroup.groupNum)

    val chunkSetIter = tsIt.flatMap { p =>
      // TODO re-enable following assertion. Am noticing that monix uses TrampolineExecutionContext
      // causing the iterator to be consumed synchronously in some cases. It doesnt
      // seem to be consistent environment to environment.
      // assertThreadName(IOSchedName)

      /* Step 2: Make chunks to be flushed for each timeseries */
      val chunks = p.makeFlushChunks(blockHolder)

      /* VERY IMPORTANT: This block is lazy and is executed when chunkSetIter is consumed
         in writeChunksFuture below */

      /* Step 4: Update endTime of all tsKeys that stopped ingesting in this flush period. */
      updateIndexWithEndTime(p, chunks, flushGroup.dirtyTsToFlush)
      chunks
    }

    // Note that all cassandra writes below  will have included retries. Failures after retries will imply data loss
    // in order to keep the ingestion moving. It is important that we don't fall back far behind.

    /* Step 1: Kick off timeseries iteration to persist chunks to column store */
    val writeChunksFuture = writeChunks(flushGroup, chunkSetIter, tsIt, blockHolder)

    /* Step 5.2: We flush dirty timeseries keys in the one designated group for each shard.
     * We recover future since we want to proceed to write dirty timeseries keys even if chunk flush failed.
     * This is done after writeChunksFuture because chunkSetIter is lazy. More tsKeys could
     * be added during iteration due to endTime detection
     */
    val writeDirtyTsKeysFuture = writeChunksFuture.recover {case _ => Success}
      .flatMap( _=> writeDirtyTsKeys(flushGroup))

    /* Step 6: Checkpoint after dirty timeseries keys and chunks are flushed */
    val result = Future.sequence(Seq(writeChunksFuture, writeDirtyTsKeysFuture)).map {
      _.find(_.isInstanceOf[ErrorResponse]).getOrElse(Success)
    }.flatMap {
      case Success           => commitCheckpoint(ref, shardNum, flushGroup)
      case er: ErrorResponse => Future.successful(er)
    }.recover { case e =>
      logger.error(s"Internal Error when persisting chunks in dataset=$ref shard=$shardNum - should " +
        s"have not reached this state", e)
      DataDropped
    }
    result.onComplete { resp =>
      assertThreadName(IngestSchedName)
      try {
        // COMMENTARY ON BUG FIX DONE: Mark used blocks as reclaimable even on failure. Even if cassandra write fails
        // or other errors occur, we cannot leave blocks as not reclaimable and also release the factory back into pool.
        // Earlier, we were not calling this with the hope that next use of the blockMemFactory will mark them
        // as reclaimable. But the factory could be used for a different flush group. Not the same one. It can
        // succeed, and the wrong blocks can be marked as reclaimable.
        // Can try out tracking unreclaimed blockMemFactories without releasing, but it needs to be separate PR.
        blockHolder.markFullBlocksReclaimable()
        blockFactoryPool.release(blockHolder)
        flushDoneTasks(flushGroup, resp)
        shardStats.chunkFlushTaskLatency.record(System.currentTimeMillis() - flushStart)
      } catch { case e: Throwable =>
        logger.error(s"Error when wrapping up doFlushSteps in dataset=$ref shard=$shardNum", e)
      }
    }(ingestSched)
    // Note: The data structures accessed by flushDoneTasks can only be safely accessed by the
    //       ingestion thread, hence the onComplete steps are run from that thread.
    Task.fromFuture(result)
  }

  protected def flushDoneTasks(flushGroup: FlushGroup, resTry: Try[Response]): Unit = {
    assertThreadName(IngestSchedName)
    resTry.foreach { resp =>
      logger.info(s"Flush of dataset=$ref shard=$shardNum group=${flushGroup.groupNum} " +
        s"flushWatermark=${flushGroup.flushWatermark} response=$resp offset=${_offset}")
    }
    updateGauges()
  }

  // scalastyle:off method.length
  private def writeDirtyTsKeys(flushGroup: FlushGroup): Future[Response] = {
    assertThreadName(IOSchedName)
    val tsKeyRecords = InMemTimeSeriesIterator2(flushGroup.dirtyTsToFlush).map(toTsKeyRecord)
    val updateHour = System.currentTimeMillis() / 1000 / 60 / 60
    colStore.writeTsKeys(ref, shardNum,
                           Observable.fromIterator(tsKeyRecords),
                           storeConfig.diskTTLSeconds, updateHour).map { resp =>
      if (flushGroup.dirtyTsToFlush.length > 0) {
        logger.info(s"Finished flush of tsKeys numTsKeys=${flushGroup.dirtyTsToFlush.length}" +
          s" resp=$resp for dataset=$ref shard=$shardNum")
        shardStats.numDirtyTsKeysFlushed.increment(flushGroup.dirtyTsToFlush.length)
      }
      resp
    }.recover { case e =>
      logger.error(s"Internal Error when persisting timeseries keys in dataset=$ref shard=$shardNum - " +
        "should have not reached this state", e)
      DataDropped
    }
  }
  // scalastyle:on method.length

  private def writeChunks(flushGroup: FlushGroup,
                          chunkSetIt: Iterator[ChunkSet],
                          tsIt: Iterator[TimeSeries],
                          blockHolder: BlockMemFactory): Future[Response] = {
    assertThreadName(IngestSchedName)

    val chunkSetStream = Observable.fromIterator(chunkSetIt)
    logger.debug(s"Created flush ChunkSets stream for group ${flushGroup.groupNum} in " +
      s"dataset=$ref shard=$shardNum")

    colStore.write(ref, chunkSetStream, storeConfig.diskTTLSeconds).recover { case e =>
      logger.error(s"Critical! Chunk persistence failed after retries and skipped in dataset=$ref " +
        s"shard=$shardNum", e)
      shardStats.flushesFailedChunkWrite.increment()

      // Encode and free up the remainder of the WriteBuffers that have not been flushed yet.  Otherwise they will
      // never be freed.
      tsIt.foreach(_.encodeAndReleaseBuffers(blockHolder))
      // If the above futures fail with ErrorResponse because of DB failures, skip the chunk.
      // Sorry - need to drop the data to keep the ingestion moving
      DataDropped
    }
  }

  private[memstore] def updateTsEndTimeInIndex(p: TimeSeries, endTime: Long): Unit =
    tsKeyTagValueIndex.updateTsKeyWithEndTime(p.tsKeyBytes, p.tsId, endTime)()

  private def updateIndexWithEndTime(p: TimeSeries,
                                     tsFlushChunks: Iterator[ChunkSet],
                                     dirtyTsIds: debox.Buffer[Int]): Unit = {
    // TODO re-enable following assertion. Am noticing that monix uses TrampolineExecutionContext
    // causing the iterator to be consumed synchronously in some cases. It doesnt
    // seem to be consistent environment to environment.
    //assertThreadName(IOSchedName)

    // Below is coded to work concurrently with logic in getOrAddTimeSeriesAndIngest
    // where we try to activate an inactive time series
    activelyIngesting.synchronized {
      if (tsFlushChunks.isEmpty && p.ingesting) {
        var endTime = p.timestampOfLatestSample
        if (endTime == -1) endTime = System.currentTimeMillis() // this can happen if no sample after reboot
        updateTsEndTimeInIndex(p, endTime)
        dirtyTsIds += p.tsId
        activelyIngesting -= p.tsId
        markTsAsNotIngesting(p, odp = false)
      }
    }
  }

  protected[memstore] def markTsAsNotIngesting(p: TimeSeries, odp: Boolean): Unit = {
    p.ingesting = false
    shardStats.evictableTsKeysSize.increment()
    if (odp) evictableOdpTsIds.add(p.tsId) else evictableTsIds.add(p.tsId)
  }

  private def commitCheckpoint(ref: DatasetRef, shardNum: Int, flushGroup: FlushGroup): Future[Response] = {
    assertThreadName(IOSchedName)
    // negative checkpoints are refused by Kafka, and also offsets should be positive
    if (flushGroup.flushWatermark > 0) {
      val fut = metastore.writeCheckpoint(ref, shardNum, flushGroup.groupNum, flushGroup.flushWatermark).map { r =>
        shardStats.flushesSuccessful.increment()
        r
      }.recover { case e =>
        logger.error(s"Critical! Checkpoint persistence skipped in dataset=$ref shard=$shardNum", e)
        shardStats.flushesFailedOther.increment()
        // skip the checkpoint write
        // Sorry - need to skip to keep the ingestion moving
        DataDropped
      }
      // Update stats
      if (_offset >= 0) shardStats.offsetLatestInMem.update(_offset)
      groupWatermark(flushGroup.groupNum) = flushGroup.flushWatermark
      val maxWatermark = groupWatermark.max
      val minWatermark = groupWatermark.min
      if (maxWatermark >= 0) shardStats.offsetLatestFlushed.update(maxWatermark)
      if (minWatermark >= 0) shardStats.offsetEarliestFlushed.update(minWatermark)
      fut
    } else {
      Future.successful(NotApplied)
    }
  }

  private[memstore] val addTimeSeriesDisabled = AtomicBoolean(false)

  // scalastyle:off null
  private[filodb] def getOrAddTimeSeriesForIngestion(recordBase: Any, recordOff: Long,
                                                    group: Int, schema: Schema) = {
    var timeseries = tsKeyToTs.getWithIngestBR(recordBase, recordOff, schema)
    if (timeseries == null) {
      timeseries = addTimeseriesForIngestion(recordBase, recordOff, schema, group)
    }
    timeseries
  }
  // scalastyle:on

  /**
    * Looks up the previously assigned tsId of a possibly evicted timeseries.
    * @return tsId >=0 if one is found, CREATE_NEW_TS_ID (-1) if not found.
    */
  private def lookupPreviouslyAssignedTsId(tsKeyBase: Array[Byte], tsKeyOffset: Long): Int = {
    assertThreadName(IngestSchedName)
    shardStats.evictedTsKeyBloomFilterQueries.increment()

    val mightContain = evictedTsKeysBF.synchronized {
      if (!evictedTsKeysBFDisposed) {
        evictedTsKeysBF.mightContain(TsKey(tsKeyBase, tsKeyOffset))
      } else {
        false
      }
    }

    if (mightContain) {
      tsKeyTagValueIndex.tsIdFromTsKeySlow(tsKeyBase, tsKeyOffset)
        .getOrElse {
          shardStats.evictedTsKeyBloomFilterFalsePositives.increment()
          CREATE_NEW_TS_ID
        }
    } else CREATE_NEW_TS_ID
  }

  /**
    * Adds new timeseries with appropriate tsId. If it is a newly seen tsKey, then new tsId is assigned.
    * If it is a previously seen tsKey that is already in index, it reassigns same tsId so that indexes
    * are still valid.
    *
    * This method also updates lucene index and dirty timeseries keys properly.
    */
  private def addTimeseriesForIngestion(recordBase: Any, recordOff: Long, schema: Schema, group: Int) = {
    assertThreadName(IngestSchedName)
    // TODO: remove when no longer needed - or figure out how to log only for tracing timeseries
    logger.trace(s"Adding ingestion record details: ${schema.ingestionSchema.debugString(recordBase, recordOff)}")
    val tsKeyOffset = schema.comparator.buildTsKeyFromIngest(recordBase, recordOff, tsKeyBuilder)
    val previousTsId = lookupPreviouslyAssignedTsId(tsKeyArray, tsKeyOffset)
    // TODO: remove when no longer needed
    logger.trace(s"Adding timeseries key details: ${schema.tsKeySchema.debugString(tsKeyArray, tsKeyOffset)}")
    val newTs = createNewTimeSeries(tsKeyArray, tsKeyOffset, group, previousTsId, schema, false)
    if (newTs != OutOfMemTs) {
      val tsId = newTs.tsId
      val startTime = schema.ingestionSchema.getLong(recordBase, recordOff, 0)
      if (previousTsId == CREATE_NEW_TS_ID) {
        // add new lucene entry if this tsKey was never seen before
        // causes endTime to be set to Long.MaxValue
        tsKeyTagValueIndex.addTsKey(newTs.tsKeyBytes, tsId, startTime)()
        val shardKey = schema.tsKeySchema.colValues(newTs.tsKeyBase, newTs.tsKeyOffset,
          schema.options.shardKeyColumns)
        captureTimeseriesCount(schema, shardKey, 1)
        if (storeConfig.meteringEnabled) {
          cardTracker.incrementCount(shardKey)
        }
      } else {
        // newly created timeseries is re-ingesting now, so update endTime
        updateTsEndTimeInIndex(newTs, Long.MaxValue)
      }
      dirtyTimeSeriesForIndexFlush += tsId // marks this timeseries as dirty so startTime is flushed
      activelyIngesting.synchronized {
        activelyIngesting += tsId
        newTs.ingesting = true
      }
      val stamp = tsSetLock.writeLock()
      try {
        tsKeyToTs.add(newTs)
      } finally {
        tsSetLock.unlockWrite(stamp)
      }
    }
    newTs
  }

  /**
    * Retrieves or creates a new TimeSeries, updating indices, then ingests the sample from record.
    * timeseries portion of ingest BinaryRecord is used to look up existing TimeSeries.
    * Copies the timeseries portion of the ingest BinaryRecord to offheap write buffer memory.
    * NOTE: ingestion is skipped if there is an error allocating WriteBuffer space.
    * @param recordBase the base of the ingestion BinaryRecord
    * @param recordOff the offset of the ingestion BinaryRecord
    * @param group the group number, from abs(record.tsHash % numGroups)
    */
  def getOrAddTimeSeriesAndIngest(ingestionTime: Long,
                                  recordBase: Any, recordOff: Long,
                                  group: Int, schema: Schema): Unit = {
    assertThreadName(IngestSchedName)
    try {
      val ts: FiloTimeSeries = getOrAddTimeSeriesForIngestion(recordBase, recordOff, group, schema)
      if (ts == OutOfMemTs) {
        disableAddTimeSeries()
      }
      else {
        val tsp = ts.asInstanceOf[TimeSeries]
        brRowReader.schema = schema.ingestionSchema
        brRowReader.recordOffset = recordOff
        tsp.ingest(ingestionTime, brRowReader, blockFactoryPool.checkoutForOverflow(group),
          storeConfig.timeAlignedChunksEnabled, flushBoundaryMillis, acceptDuplicateSamples, maxChunkTime)
        // Below is coded to work concurrently with logic in updateIndexWithEndTime
        // where we try to de-activate an active time series
        if (!tsp.ingesting) {
          // DO NOT use activelyIngesting to check above condition since it is slow and is called for every sample
          activelyIngesting.synchronized {
            if (!tsp.ingesting) {
              // time series was inactive and has just started re-ingesting
              updateTsEndTimeInIndex(ts.asInstanceOf[TimeSeries], Long.MaxValue)
              dirtyTimeSeriesForIndexFlush += ts.tsId
              activelyIngesting += ts.tsId
              tsp.ingesting = true
            }
          }
        }
      }
    } catch {
      case e: OutOfOffheapMemoryException => disableAddTimeSeries()
      case e: Exception =>
        shardStats.dataDropped.increment()
        logger.error(s"Unexpected ingestion err in dataset=$ref " +
          s"shard=$shardNum ts=${schema.ingestionSchema.debugString(recordBase, recordOff)}", e)
    }
  }

  private def shouldTrace(tsKeyAddr: Long): Boolean = {
    tracedTsFilters.nonEmpty && {
      val tsKeyPairs = schemas.ts.binSchema.toStringPairs(UnsafeUtils.ZeroPointer, tsKeyAddr)
      tracedTsFilters.forall(p => tsKeyPairs.contains(p))
    }
  }

  /**
    * Creates new getOrAddTimeSeriesForIngestion and adds them to the shard data structures. DOES NOT update
    * lucene index. It is the caller's responsibility to add or skip that step depending on the situation.
    *
    * @param useTsId pass CREATE_NEW_TS_ID to force creation of new tsId instead of using one that is passed in
    */
  protected def createNewTimeSeries(tsKeyBase: Array[Byte], tsKeyOffset: Long,
                                    group: Int, useTsId: Int, schema: Schema,
                                    odp: Boolean,
                                    initMapSize: Int = initInfoMapSize): TimeSeries = {
    assertThreadName(IngestSchedName)
    if (tsIdToTsMap.size() >= maxTimeSeriesCount) {
      disableAddTimeSeries()
    }
    // Check and evict, if after eviction we still don't have enough memory, then don't proceed
    // We do not evict in ODP cases since we do not want eviction of ODP timeseries that we already paged
    // in for same query. Calling this as ODP cannibalism. :-)
    if (addTimeSeriesDisabled() && !odp) evictForHeadroom()
    if (addTimeSeriesDisabled()) OutOfMemTs
    else {
      // TimeSeriesKey is copied to offheap bufferMemory and stays there until it is freed
      // NOTE: allocateAndCopy and allocNew below could fail if there isn't enough memory.  It is CRUCIAL
      // that min-write-buffers-free setting is large enough to accommodate the below use cases ALWAYS
      val (_, tsKeyAddr, _) = BinaryRegionLarge.allocateAndCopy(tsKeyBase, tsKeyOffset, bufferMemoryManager)
      val tsId = if (useTsId == CREATE_NEW_TS_ID) createTsId() else useTsId
      val pool = bufferPools(schema.schemaHash)
      val newTs = if (shouldTrace(tsKeyAddr)) {
        logger.debug(s"Adding TracingTimeSeries dataset=$ref shard=$shardNum group=$group tsId=$tsId")
        new TracingTimeSeries(
          tsId, ref, schema, tsKeyAddr, shardNum, pool, shardStats, bufferMemoryManager, initMapSize)
      } else {
        new TimeSeries(
          tsId, schema, tsKeyAddr, shardNum, pool, shardStats, bufferMemoryManager, initMapSize)
      }
      tsIdToTsMap.put(tsId, newTs)
      shardStats.numTsCreated.increment()
      tsGroups(group).set(tsId)
      newTs
    }
  }

  /**
   * Called during ingestion if we run out of memory while creating TimeSeries, or
   * if we went above the maxTimeSeriesLimit without being able to evict. Can happen if
   * within 1 minute (time between headroom tasks) too many new time series was added and it
   * went beyond limit of max timeseries (or ran out of native memory).
   *
   * When timeseries addition is disabled, headroom task begins to run inline during ingestion
   * with force-eviction enabled.
   */
  private def disableAddTimeSeries(): Unit = {
    assertThreadName(IngestSchedName)
    if (addTimeSeriesDisabled.compareAndSet(false, true))
      logger.warn(s"dataset=$ref shard=$shardNum: Out of native memory, or max timeseries reached. " +
        s"Not able to evict enough; adding timeseries disabled")
    shardStats.dataDropped.increment()
  }

  /**
   * Returns a new non-negative timeseries ID which isn't used by any existing parition. A negative
   * timeseries ID wouldn't work with bitmaps.
   */
  private def createTsId(): Int = {
    assertThreadName(IngestSchedName)
    val id = nextTsId

    // It's unlikely that timeseries IDs will wrap around, and it's unlikely that collisions
    // will be encountered. In case either of these conditions occur, keep incrementing the id
    // until no collision is detected. A given shard is expected to support up to 1M actively
    // ingesting timeseries, and so in the worst case, the loop might run for up to ~100ms.
    // Afterwards, a complete wraparound is required for collisions to be detected again.

    do {
      nextTsId += 1
      if (nextTsId < 0) {
        nextTsId = 0
        logger.info(s"dataset=$ref shard=$shardNum nextTsId has wrapped around to 0 again")
      }
    } while (tsIdToTsMap.containsKey(nextTsId))

    id
  }

  def analyzeAndLogCorruptPtr(cve: CorruptVectorException): Unit =
    logger.error(cve.getMessage + "\n" + BlockDetective.stringReport(cve.ptr, blockStore, blockFactoryPool))

  /**
   * Check and evict timeseries to free up memory and heap space.  NOTE: This must be called in the ingestion
   * stream so that there won't be concurrent other modifications.  Ideally this is called when trying to add timeseries
   *
   * Expected to be called from evictForHeadroom method, only after obtaining evictionLock
   * to prevent wrong query results.
   *
   * @return true if able to evict enough or there was already space, false if not able to evict and not enough mem
   */
  // scalastyle:off method.length
  private[memstore] def makeSpaceForNewTimeSeries(forceEvict: Boolean): Boolean = {
    assertThreadName(IngestSchedName)
    val numTsToEvict = if (forceEvict) (maxTimeSeriesCount * ensureTspHeadroomPercent / 100).toInt
    else evictionPolicy.numTsToEvictForHeadroom(tsKeyToTs.size, maxTimeSeriesCount, bufferMemoryManager)
    if (numTsToEvict > 0) {
      val tsIdsToEvict = tsToEvict(numTsToEvict)
      if (tsIdsToEvict.isEmpty) {
        logger.warn(s"dataset=$ref shard=$shardNum: No timeseries to evict but we are still low on space. " +
          s"DATA WILL BE DROPPED")
        return false
      }

      // Finally, prune timeseries and keyMap data structures
      logger.info(s"Evicting timeseries from dataset=$ref shard=$shardNum ...")
      val intIt = tsIdsToEvict.intIterator
      var numTsEvicted = 0
      var numTsSkipped = 0
      val evictedTsIds = new EWAHCompressedBitmap()
      while (intIt.hasNext) {
        val ts = tsIdToTsMap.get(intIt.next)
        if (ts != UnsafeUtils.ZeroPointer) {
          if (!ts.ingesting) { // could have started re-ingesting after it got into evictableTsIds queue
            logger.debug(s"Evicting tsId=${ts.tsId} ${ts.stringTsKey} " +
              s"from dataset=$ref shard=$shardNum")
            // add the evicted tsKey to a bloom filter so that we are able to quickly
            // find out if a tsId has been assigned to an ingesting tsKey before a more expensive lookup.
            evictedTsKeysBF.synchronized {
              if (!evictedTsKeysBFDisposed) {
                evictedTsKeysBF.add(TsKey(ts.tsKeyBase, ts.tsKeyOffset))
              }
            }
            // The previously created tsKey is just meant for bloom filter and will be GCed
            removeTimeseries(ts)
            evictedTsIds.set(ts.tsId)
            numTsEvicted += 1
          } else {
            numTsSkipped += 1
          }
        } else {
          numTsSkipped += 1
        }
      }
      // Pruning group bitmaps.
      for { group <- 0 until numGroups } {
        tsGroups(group) = tsGroups(group).andNot(evictedTsIds)
      }
      val elemCount = evictedTsKeysBF.synchronized {
        if (!evictedTsKeysBFDisposed) evictedTsKeysBF.approximateElementCount() else 0
      }
      shardStats.evictedPkBloomFilterSize.update(elemCount)
      logger.info(s"dataset=$ref shard=$shardNum: evicted $numTsEvicted timeseries, skipped $numTsSkipped")
      shardStats.numTsEvicted.increment(numTsEvicted)
    }
    true
  }

  //scalastyle:on

  // Permanently removes the given timeseries ID from our in-memory data structures
  // Also frees timeseries key if necessary
  private def removeTimeseries(ts: TimeSeries): Unit = {
    assertThreadName(IngestSchedName)
    val stamp = tsSetLock.writeLock()
    try {
      tsKeyToTs.remove(ts)
    } finally {
      tsSetLock.unlockWrite(stamp)
    }
    if (tsIdToTsMap.remove(ts.tsId, ts)) {
      ts.shutdown()
    }
  }

  private def tsToEvict(numTsToEvict: Int): EWAHCompressedBitmap = {
    val tsIdsToEvict = new EWAHCompressedBitmap()
    var i = 0
    while (i < numTsToEvict && !evictableOdpTsIds.isEmpty) {
      val tsId = evictableOdpTsIds.remove()
      tsIdsToEvict.set(tsId)
      logger.debug(s"Preparing to evict ODP tsId=$tsIdsToEvict")
      i += 1
    }
    while (i < numTsToEvict && !evictableTsIds.isEmpty) {
      val tsId = evictableTsIds.remove()
      tsIdsToEvict.set(tsId)
      logger.debug(s"Preparing to evict tsId=$tsIdsToEvict")
      i += 1
    }
    shardStats.evictableTsKeysSize.decrement(i)
    tsIdsToEvict
  }

  private[core] def getTimeSeries(tsKey: Array[Byte]): Option[TimeSeries] = {
    var ts: Option[FiloTimeSeries] = None
    // Access the timeseries set optimistically. If nothing acquired the write lock, then
    // nothing changed in the set, and the timeseries object is the correct one.
    var stamp = tsSetLock.tryOptimisticRead()
    if (stamp != 0) {
      ts = tsKeyToTs.getWithTsKeyBR(tsKey, UnsafeUtils.arayOffset, schemas.ts)
    }
    if (!tsSetLock.validate(stamp)) {
      // Because the stamp changed, the write lock was acquired and the set likely changed.
      // Try again with a full read lock, which will block if necessary so as to not run
      // concurrently with any thread making changes to the set. This guarantees that
      // the correct timeseries is returned.
      stamp = tsSetLock.readLock()
      try {
        ts = tsKeyToTs.getWithTsKeyBR(tsKey, UnsafeUtils.arayOffset, schemas.ts)
      } finally {
        tsSetLock.unlockRead(stamp)
      }
    }
    ts.map(_.asInstanceOf[TimeSeries])
  }

  protected def schemaIdFromTsId(tsId: Int): Int = {
    tsIdToTsMap.get(tsId) match {
      case TimeSeriesShard.OutOfMemTs =>
        tsKeyTagValueIndex.tsKeyFromTsId(tsId).map { pkBytesRef =>
          val unsafeKeyOffset = TsKeyLuceneIndex.bytesRefToUnsafeOffset(pkBytesRef.offset)
          RecordSchema.schemaID(pkBytesRef.bytes, unsafeKeyOffset)
        }.getOrElse(-1)
      case p: TimeSeries => p.schema.schemaHash
    }
  }

  /**
    * Looks up timeseries and schema info from ScanMethods, usually by doing a Lucene search.
    * Also returns detailed information about what is in memory and not, and does schema discovery.
    */
  def lookupTimeSeries(tsMethod: TimeseriesScanMethod,
                       chunkMethod: ChunkScanMethod,
                       querySession: QuerySession): TsLookupResult = {
    querySession.lock = Some(evictionLock)
    evictionLock.acquireSharedLock()
    val metricShardKeys = schemas.ts.options.shardKeyColumns.dropRight(1)
    // any exceptions thrown here should be caught by a wrapped Task.
    // At the end, MultiSchemaPartitionsExec.execute releases the lock when the task is complete
    tsMethod match {
      case SingleTimeseriesScan(timeseries, _) =>
        val tsIds = debox.Buffer.empty[Int]
        getTimeSeries(timeseries).foreach(p => tsIds += p.tsId)
        TsLookupResult(shardNum, chunkMethod, tsIds, Some(RecordSchema.schemaID(timeseries)),
          queriedChunksCounter = shardStats.chunksQueried)
      case MultiTimeseriesScan(tsKeys, _)   =>
        val tsIds = debox.Buffer.empty[Int]
        tsKeys.flatMap(getTimeSeries).foreach(p => tsIds += p.tsId)
        TsLookupResult(shardNum, chunkMethod, tsIds, tsKeys.headOption.map(RecordSchema.schemaID),
          queriedChunksCounter = shardStats.chunksQueried)
      case FilteredTimeseriesScan(_, filters) =>
        val chunksQueriedMetric = if (shardKeyLevelQueryMetricsEnabled) {
          val metricTags = metricShardKeys.map { col =>
            filters.collectFirst {
              case ColumnFilter(c, Filter.Equals(filtVal: String)) if c == col => s"metric${col}tag" -> filtVal
            }.getOrElse(col -> "unknown")
          }.toMap
          shardStats.chunksQueriedByShardKey.withTags(TagSet.from(metricTags))
        } else shardStats.chunksQueried
        // No matter if there are filters or not, need to run things through Lucene so we can discover potential
        // TimeSeries to read back from disk
        val matches = tsKeyTagValueIndex.tsIdsFromFilters(filters, chunkMethod.startTime, chunkMethod.endTime)
        shardStats.queryTimeRangeMins.record((chunkMethod.endTime - chunkMethod.startTime) / 60000 )

        Kamon.currentSpan().tag(s"num-partitions-from-index-$shardNum", matches.length)

        // first find out which timeseries are being queried for data not in memory
        val firstTsId = if (matches.isEmpty) None else Some(matches(0))
        val _schema = firstTsId.map(schemaIdFromTsId)
        val it1 = InMemTimeSeriesIterator2(matches)
        val tsIdsToPage = it1.filter(_.earliestTime > chunkMethod.startTime).map(_.tsId)
        val tsIdsNotInMem = it1.skippedTsIds
        Kamon.currentSpan().tag(s"num-partitions-not-in-memory-$shardNum", tsIdsNotInMem.length)
        val startTimes = if (tsIdsToPage.nonEmpty) {
          val st = tsKeyTagValueIndex.startTimeFromTsIds(tsIdsToPage)
          logger.debug(s"Some timeseries have earliestTime > queryStartTime(${chunkMethod.startTime}); " +
            s"startTime lookup for query in dataset=$ref shard=$shardNum " +
            s"resulted in startTimes=$st")
          st
        }
        else {
          logger.debug(s"StartTime lookup was not needed. All timeseries data for query in dataset=$ref " +
            s"shard=$shardNum are in memory")
          debox.Map.empty[Int, Long]
        }
        // now provide an iterator that additionally supplies the startTimes for
        // those timeseries that may need to be paged
        TsLookupResult(shardNum, chunkMethod, matches, _schema, startTimes, tsIdsNotInMem,
          Nil, chunksQueriedMetric)
    }
  }

  def scanTimeSeries(iterResult: TsLookupResult,
                     colIds: Seq[Types.ColumnId],
                     querySession: QuerySession): Observable[ReadableTimeSeries] = {

    val tsIter = InMemTimeSeriesIterator2(iterResult.tsInMemory)
    Observable.fromIterator(tsIter.map { p =>
      shardStats.numTsQueried.increment()
      p
    })
  }

  /**
   * Calculate lock timeout based on headroom space available.
   * Lower the space available, longer the timeout.
   */
  def getHeadroomLockTimeout(currentFreePercent: Double, ensurePercent: Double): Int = {
    // Ramp up the timeout as the current headroom shrinks. Max timeout per attempt is a little
    // over 2 seconds, and the total timeout can be double that, for a total of 4 seconds.
    ((1.0 - (currentFreePercent / ensurePercent)) * EvictionLock.maxTimeoutMillis).toInt
  }

  /**
   * This task is run every 1 minute to make headroom in memory. It is also called
   * from createNewTimeSeries method (inline during ingestion) if we run out of space between
   * headroom task runs.
   * This method acquires eviction lock and reclaim block and TSP memory.
   * If addTimeSeries is disabled, we force eviction even if we cannot acquire eviction lock.
   * @return true if eviction was attempted
   */
  private[memstore] def evictForHeadroom(): Boolean = {

    // measure how much headroom we have
    val blockStoreCurrentFreePercent = blockStore.currentFreePercent
    val tspCountFreePercent = (maxTimeSeriesCount - tsIdToTsMap.size.toDouble) / maxTimeSeriesCount
    val nativeMemFreePercent = bufferMemoryManager.numFreeBytes.toDouble / bufferMemoryManager.upperBoundSizeInBytes

    // calculate lock timeouts based on free percents and target headroom to maintain. Lesser the headroom,
    // higher the timeout. Choose highest among the three.
    val blockTimeoutMs = getHeadroomLockTimeout(blockStoreCurrentFreePercent, ensureBlockHeadroomPercent)
    val tspTimeoutMs: Int = getHeadroomLockTimeout(tspCountFreePercent, ensureTspHeadroomPercent)
    val nativeMemTimeoutMs = getHeadroomLockTimeout(nativeMemFreePercent, ensureNativeMemHeadroomPercent)
    val highestTimeoutMs = Math.max(Math.max(blockTimeoutMs, tspTimeoutMs), nativeMemTimeoutMs)

    // whether to force evict even if lock cannot be acquired, if situation is dire
    val forceEvict = addTimeSeriesDisabled.get

    // do only if one of blocks or TSPs need eviction or if addition of timeseries disabled
    if (highestTimeoutMs > 0 || forceEvict) {
      val start = System.nanoTime()
      val timeoutMs = if (forceEvict) EvictionLock.direCircumstanceTimeoutMillis else highestTimeoutMs
      logger.info(s"Preparing to evictForHeadroom on dataset=$ref shard=$shardNum since " +
        s"blockStoreCurrentFreePercent=$blockStoreCurrentFreePercent tspCountFreePercent=$tspCountFreePercent " +
        s"nativeMemFreePercent=$nativeMemFreePercent forceEvict=$forceEvict timeoutMs=$timeoutMs")
      val acquired = evictionLock.tryExclusiveReclaimLock(timeoutMs)
      // if forceEvict is true, then proceed even if we dont have a lock
      val jobDone = if (forceEvict || acquired) {
        if (!acquired) logger.error(s"Since addTimeSeriesDisabled is true, proceeding with reclaim " +
          s"even though eviction lock couldn't be acquired with final timeout of $timeoutMs ms. Trading " +
          s"off possibly wrong query results (due to old inactive timeseries that would be evicted " +
          s"and skipped) in order to unblock ingestion and stop data loss. LockState: $evictionLock")
        try {
          if (blockTimeoutMs > 0) {
            blockStore.ensureHeadroom(ensureBlockHeadroomPercent)
          }
          if (tspTimeoutMs > 0 || nativeMemTimeoutMs > 0) {
            if (makeSpaceForNewTimeSeries(forceEvict)) addTimeSeriesDisabled := false
          }
        } finally {
          if (acquired) evictionLock.releaseExclusive()
        }
        true
      } else {
        false
      }
      val stall = System.nanoTime() - start
      shardStats.memstoreEvictionStall.increment(stall)
      jobDone
    } else {
      true
    }
  }

  private def startHeadroomTask(sched: Scheduler) = {
    sched.scheduleWithFixedDelay(1, 1, TimeUnit.MINUTES, new Runnable {
      def run() = {
        evictForHeadroom()
      }
    })
  }

  /**
    * Please use this for testing only - reclaims ALL used offheap blocks.  Maybe you are trying to test
    * on demand paging.
    */
  private[filodb] def reclaimAllBlocksTestOnly() = blockStore.reclaimAll()

  /**
    * Reset all state in this shard.  Memory is not released as once released, then this class
    * cannot be used anymore (except timeseries key/chunkmap state is removed.)
    */
  def reset(): Unit = {
    logger.info(s"Clearing all MemStore state for dataset=$ref shard=$shardNum")
    ingestSched.executeTrampolined { () =>
      tsIdToTsMap.values.asScala.foreach(removeTimeseries)
    }
    tsKeyTagValueIndex.reset()
    // TODO unable to reset/clear bloom filter
    ingested = 0L
    for { group <- 0 until numGroups } {
      tsGroups(group) = new EWAHCompressedBitmap()
      groupWatermark(group) = Long.MinValue
    }
  }

  def shutdown(): Unit = {
    if (storeConfig.meteringEnabled) {
      cardTracker.close()
    }
    evictedTsKeysBF.synchronized {
      if (!evictedTsKeysBFDisposed) {
        evictedTsKeysBFDisposed = true
        evictedTsKeysBF.dispose()
      }
    }
    reset()   // Not really needed, but clear everything just to be consistent
    tsKeyTagValueIndex.closeIndex()
    logger.info(s"Shutting down dataset=$ref shard=$shardNum")
    /* Don't explcitly free the memory just yet. These classes instead rely on a finalize
       method to ensure that no threads are accessing the memory before it's freed.
    blockStore.releaseBlocks()
    */
    headroomTask.cancel()
    ingestSched.shutdown()
  }
}
