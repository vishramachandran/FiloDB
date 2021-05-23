package filodb.core.memstore

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.PriorityQueue

import scala.collection.JavaConverters._
import scala.collection.immutable.HashSet
import scala.collection.mutable.ArrayBuffer

import com.googlecode.javaewah.{EWAHCompressedBitmap, IntIterator}
import com.typesafe.scalalogging.StrictLogging
import java.util
import kamon.Kamon
import kamon.metric.MeasurementUnit
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document._
import org.apache.lucene.document.Field.Store
import org.apache.lucene.index._
import org.apache.lucene.search._
import org.apache.lucene.search.BooleanClause.Occur
import org.apache.lucene.store.MMapDirectory
import org.apache.lucene.util.{BytesRef, InfoStream}
import org.apache.lucene.util.automaton.RegExp
import spire.syntax.cfor._

import filodb.core.{concurrentCache, DatasetRef}
import filodb.core.Types.TsKeyPtr
import filodb.core.binaryrecord2.MapItemConsumer
import filodb.core.metadata.Column.ColumnType.{MapColumn, StringColumn}
import filodb.core.metadata.TsKeySchema
import filodb.core.query.{ColumnFilter, Filter}
import filodb.core.query.Filter._
import filodb.memory.{BinaryRegionLarge, UTF8StringMedium, UTF8StringShort}
import filodb.memory.format.{UnsafeUtils, ZeroCopyUTF8String => UTF8Str}

object TsKeyLuceneIndex {
  final val TS_ID = "__tsId__"
  final val START_TIME = "__startTime__"
  final val END_TIME = "__endTime__"
  final val TS_KEY = "__tsKey__"

  final val ignoreIndexNames = HashSet(START_TIME, TS_KEY, END_TIME, TS_ID)

  val MAX_STR_INTERN_ENTRIES = 10000
  val MAX_TERMS_TO_ITERATE = 10000

  val NOT_FOUND = -1

  def bytesRefToUnsafeOffset(bytesRefOffset: Int): Int = bytesRefOffset + UnsafeUtils.arayOffset

  def unsafeOffsetToBytesRefOffset(offset: Long): Int = offset.toInt - UnsafeUtils.arayOffset

  def tsKeyBytesRef(tsKeyBase: Array[Byte], tsKeyOffset: Long): BytesRef = {
    new BytesRef(tsKeyBase, unsafeOffsetToBytesRefOffset(tsKeyOffset),
      BinaryRegionLarge.numBytes(tsKeyBase, tsKeyOffset))
  }

  private def createTempDir(ref: DatasetRef, shardNum: Int): File = {
    val baseDir = new File(System.getProperty("java.io.tmpdir"))
    val baseName = s"tsKeyIndex-$ref-$shardNum-${System.currentTimeMillis()}-"
    val tempDir = new File(baseDir, baseName)
    tempDir.mkdir()
    tempDir
  }
}

final case class TermInfo(term: UTF8Str, freq: Int)
final case class TsKeyLuceneIndexRecord(tsKey: Array[Byte], startTime: Long, endTime: Long)

class TsKeyLuceneIndex(ref: DatasetRef,
                       schema: TsKeySchema,
                       shardNum: Int,
                       retentionMillis: Long, // only used to calculate fallback startTime
                       diskLocation: Option[File] = None
                         ) extends StrictLogging {

  import TsKeyLuceneIndex._

  val startTimeLookupLatency = Kamon.histogram("index-startTimes-for-odp-lookup-latency",
    MeasurementUnit.time.milliseconds)
    .withTag("dataset", ref.dataset)
    .withTag("shard", shardNum)

  val queryIndexLookupLatency = Kamon.histogram("index-partition-lookup-latency",
    MeasurementUnit.time.milliseconds)
    .withTag("dataset", ref.dataset)
    .withTag("shard", shardNum)

  val tsIdFromTsKeyLookupLatency = Kamon.histogram("index-ingestion-partId-lookup-latency",
    MeasurementUnit.time.milliseconds)
    .withTag("dataset", ref.dataset)
    .withTag("shard", shardNum)

  private val numTsKeyColumns = schema.columns.length
  private val indexDiskLocation = diskLocation.getOrElse(createTempDir(ref, shardNum)).toPath
  private val mMapDirectory = new MMapDirectory(indexDiskLocation)
  private val analyzer = new StandardAnalyzer()

  logger.info(s"Created lucene index for dataset=$ref shard=$shardNum at $indexDiskLocation")

  private val config = new IndexWriterConfig(analyzer)
  config.setInfoStream(new LuceneMetricsRouter(ref, shardNum))
  config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND)

  private val endTimeSort = new Sort(new SortField(END_TIME, SortField.Type.LONG),
                                     new SortField(START_TIME, SortField.Type.LONG))
  config.setIndexSort(endTimeSort)
  private val indexWriter = new IndexWriter(mMapDirectory, config)

  private val utf8ToStrCache = concurrentCache[UTF8Str, String](TsKeyLuceneIndex.MAX_STR_INTERN_ENTRIES)

  //scalastyle:off
  private val searcherManager = new SearcherManager(indexWriter, null)
  //scalastyle:on

  //start this thread to flush the segments and refresh the searcher every specific time period
  private var flushThread: ControlledRealTimeReopenThread[IndexSearcher] = _
  private val luceneDocument = new ThreadLocal[Document]()

  private val mapConsumer = new MapItemConsumer {
    def consume(keyBase: Any, keyOffset: Long, valueBase: Any, valueOffset: Long, index: Int): Unit = {
      import filodb.core._
      val key = utf8ToStrCache.getOrElseUpdate(new UTF8Str(keyBase, keyOffset + 1,
                                                           UTF8StringShort.numBytes(keyBase, keyOffset)),
                                               _.toString)
      val value = new BytesRef(valueBase.asInstanceOf[Array[Byte]],
                               unsafeOffsetToBytesRefOffset(valueOffset + 2), // add 2 to move past numBytes
                               UTF8StringMedium.numBytes(valueBase, valueOffset))
      addIndexEntry(key, value, index)
    }
  }

  /**
    * Map of tsKey column to the logic for indexing the column (aka Indexer).
    * Optimization to avoid match logic while iterating through each column of the tsKey
    */
  private final val indexers = schema.columns.zipWithIndex.map { case (c, pos) =>
    c.columnType match {
      case StringColumn => new Indexer {
        val colName = UTF8Str(c.name)
        def fromTsKey(base: Any, offset: Long, tsKeyIndex: Int): Unit = {
          val strOffset = schema.binSchema.blobOffset(base, offset, pos)
          val numBytes = schema.binSchema.blobNumBytes(base, offset, pos)
          val value = new BytesRef(base.asInstanceOf[Array[Byte]], strOffset.toInt - UnsafeUtils.arayOffset, numBytes)
          addIndexEntry(colName.toString, value, tsKeyIndex)
        }
        def getNamesValues(key: TsKeyPtr): Seq[(UTF8Str, UTF8Str)] = ??? // not used
      }
      case MapColumn => new Indexer {
        def fromTsKey(base: Any, offset: Long, tsKeyIndex: Int): Unit = {
          schema.binSchema.consumeMapItems(base, offset, pos, mapConsumer)
        }
        def getNamesValues(key: TsKeyPtr): Seq[(UTF8Str, UTF8Str)] = ??? // not used
      }
      case other: Any =>
        logger.warn(s"Column $c has type that cannot be indexed and will be ignored right now")
        NoOpIndexer
    }
  }.toArray

  def reset(): Unit = {
    indexWriter.deleteAll()
    indexWriter.commit()
  }

  def startFlushThread(flushDelayMinSeconds: Int, flushDelayMaxSeconds: Int): Unit = {

    flushThread = new ControlledRealTimeReopenThread(indexWriter,
                                                    searcherManager,
                                                    flushDelayMaxSeconds,
                                                    flushDelayMinSeconds)
    flushThread.start()
    logger.info(s"Started flush thread for lucene index on dataset=$ref shard=$shardNum")
  }

  /**
    * Find timeseries that ended ingesting before a given timestamp. Used to identify timeseries that can be purged.
    * @return matching tsIds
    */
  def tsIdsEndedBefore(endedBefore: Long): debox.Buffer[Int] = {
    val collector = new TsIdCollector()
    val deleteQuery = LongPoint.newRangeQuery(TsKeyLuceneIndex.END_TIME, 0, endedBefore)

    withNewSearcher(s => s.search(deleteQuery, collector))
    collector.result
  }

  private def withNewSearcher(func: IndexSearcher => Unit): Unit = {
    val s = searcherManager.acquire()
    try {
      func(s)
    } finally {
      searcherManager.release(s)
    }
  }

  /**
    * Delete timeseries with given tsIds
    */
  def removeTsKeys(tsIds: debox.Buffer[Int]): Unit = {
    if (!tsIds.isEmpty) {
      val terms = new util.ArrayList[BytesRef]()
      cforRange { 0 until tsIds.length } { i =>
        terms.add(new BytesRef(tsIds(i).toString.getBytes(StandardCharsets.UTF_8)))
      }
      indexWriter.deleteDocuments(new TermInSetQuery(TS_ID, terms))
    }
  }

  def indexRamBytes: Long = indexWriter.ramBytesUsed()

  /**
    * Number of documents in flushed index, excludes tombstones for deletes
    */
  def indexNumEntries: Long = indexWriter.numDocs()

  /**
    * Number of documents in flushed index, includes tombstones for deletes
    */
  def indexNumEntriesWithTombstones: Long = indexWriter.maxDoc()

  def closeIndex(): Unit = {
    logger.info(s"Closing index on dataset=$ref shard=$shardNum")
    if (flushThread != UnsafeUtils.ZeroPointer) flushThread.close()
    indexWriter.close()
  }

  /**
    * Fetch values/terms for a specific column/key/field, in order from most frequent on down.
   * Note that it iterates through all docs up to a certain limit only, so if there are too many terms
    * it will not report an accurate top k in exchange for not running too long.
    * @param fieldName the name of the column/field/key to get terms for
    * @param topK the number of top k results to fetch
    */
  def indexValues(fieldName: String, topK: Int = 100): Seq[TermInfo] = {
    // FIXME this API returns duplicate values because same value can be present in multiple lucene segments

    val freqOrder = Ordering.by[TermInfo, Int](_.freq)
    val topkResults = new PriorityQueue[TermInfo](topK, freqOrder)

    withNewSearcher { searcher =>
      val indexReader = searcher.getIndexReader
      val segments = indexReader.leaves()
      var termsRead = 0
      segments.asScala.foreach { segment =>
        val terms = segment.reader().terms(fieldName)

        //scalastyle:off
        if (terms != null) {
          val termsEnum = terms.iterator()
          var nextVal: BytesRef = termsEnum.next()
          while (nextVal != null && termsRead < MAX_TERMS_TO_ITERATE) {
            //scalastyle:on
            val valu = BytesRef.deepCopyOf(nextVal) // copy is needed since lucene uses a mutable cursor underneath
            val ret = new UTF8Str(valu.bytes, bytesRefToUnsafeOffset(valu.offset), valu.length)
            val freq = termsEnum.docFreq()
            if (topkResults.size < topK) {
              topkResults.add(TermInfo(ret, freq))
            }
            else if (topkResults.peek.freq < freq) {
              topkResults.remove()
              topkResults.add(TermInfo(ret, freq))
            }
            nextVal = termsEnum.next()
            termsRead += 1
          }
        }
      }
    }
    topkResults.toArray(new Array[TermInfo](0)).sortBy(-_.freq).toSeq
  }

  def indexNames(limit: Int): Seq[String] = {

    var ret: Seq[String] = Nil
    withNewSearcher { searcher =>
      val indexReader = searcher.getIndexReader
      val segments = indexReader.leaves()
      val iter = segments.asScala.iterator.flatMap { segment =>
        segment.reader().getFieldInfos.asScala.toIterator.map(_.name)
      }.filterNot { n => ignoreIndexNames.contains(n) }
      ret = iter.take(limit).toSeq
    }
    ret
  }

  private def addIndexEntry(labelName: String, value: BytesRef, partIndex: Int): Unit = {
    luceneDocument.get().add(new StringField(labelName, value, Store.NO))
  }

  def addTsKey(tsKeyOnHeapBytes: Array[Byte],
               tsId: Int,
               startTime: Long,
               endTime: Long = Long.MaxValue,
               tsKeyBytesRefOffset: Int = 0)
              (tsKeyNumBytes: Int = tsKeyOnHeapBytes.length): Unit = {
    val document = makeDocument(tsKeyOnHeapBytes, tsKeyBytesRefOffset, tsKeyNumBytes, tsId, startTime, endTime)
    logger.debug(s"Adding document ${tsKeyString(tsId, tsKeyOnHeapBytes, tsKeyBytesRefOffset)} " +
                 s"with startTime=$startTime endTime=$endTime into dataset=$ref shard=$shardNum")
    indexWriter.addDocument(document)
  }

  def upsertTsKey(tsKeyOnHeapBytes: Array[Byte],
                  tsId: Int,
                  startTime: Long,
                  endTime: Long = Long.MaxValue,
                  tsKeyBytesRefOffset: Int = 0)
                 (tsKeyNumBytes: Int = tsKeyOnHeapBytes.length): Unit = {
    val document = makeDocument(tsKeyOnHeapBytes, tsKeyBytesRefOffset, tsKeyNumBytes, tsId, startTime, endTime)
    logger.debug(s"Upserting document ${tsKeyString(tsId, tsKeyOnHeapBytes, tsKeyBytesRefOffset)} " +
                 s"with startTime=$startTime endTime=$endTime into dataset=$ref shard=$shardNum")
    indexWriter.updateDocument(new Term(TS_ID, tsId.toString), document)
  }

  private def tsKeyString(tsId: Int,
                          tsKeyOnHeapBytes: Array[Byte],
                          tsKeyBytesRefOffset: Int = 0): String = {
    val tsHash = schema.binSchema.tsHash(tsKeyOnHeapBytes, bytesRefToUnsafeOffset(tsKeyBytesRefOffset))
    //scalastyle:off
    s"shard=$shardNum tsId=$tsId tsHash=$tsHash [${
      TimeSeries.tsKeyString(schema, tsKeyOnHeapBytes, bytesRefToUnsafeOffset(tsKeyBytesRefOffset))
    }]"
    //scalastyle:on
  }

  private def makeDocument(tsKeyOnHeapBytes: Array[Byte],
                           tsKeyBytesRefOffset: Int,
                           tsKeyNumBytes: Int,
                           tsId: Int, startTime: Long, endTime: Long): Document = {
    val document = new Document()
    // TODO We can use RecordSchema.toStringPairs to get the name/value pairs from tsKey.
    // That is far more simpler with much of the logic abstracted out.
    // Currently there is a bit of leak in abstraction of Binary Record processing in this class.

    luceneDocument.set(document) // threadlocal since we are not able to pass the document into mapconsumer
    cforRange { 0 until numTsKeyColumns } { i =>
      indexers(i).fromTsKey(tsKeyOnHeapBytes, bytesRefToUnsafeOffset(tsKeyBytesRefOffset), tsId)
    }
    // tsId
    document.add(new StringField(TS_ID, tsId.toString, Store.NO)) // cant store as an IntPoint because of lucene bug
    document.add(new NumericDocValuesField(TS_ID, tsId))
    // tsKey
    val bytesRef = new BytesRef(tsKeyOnHeapBytes, tsKeyBytesRefOffset, tsKeyNumBytes)
    document.add(new BinaryDocValuesField(TS_KEY, bytesRef))
    // startTime
    document.add(new LongPoint(START_TIME, startTime))
    document.add(new NumericDocValuesField(START_TIME, startTime))
    // endTime
    document.add(new LongPoint(END_TIME, endTime))
    document.add(new NumericDocValuesField(END_TIME, endTime))

    luceneDocument.remove()
    document
  }

  /**
    * Called when TimeSeries needs to be created when on-demand-paging from a
    * tsId that does not exist on heap
    */
  def tsKeyFromTsId(tsId: Int): Option[BytesRef] = {
    val collector = new SingleTsKeyCollector()
    withNewSearcher(s => s.search(new TermQuery(new Term(TS_ID, tsId.toString)), collector) )
    Option(collector.singleResult)
  }

  /**
    * Called when a document is updated with new endTime
    */
  def startTimeFromTsId(tsId: Int): Long = {
    val collector = new NumericDocValueCollector(TsKeyLuceneIndex.START_TIME)
    withNewSearcher(s => s.search(new TermQuery(new Term(TS_ID, tsId.toString)), collector))
    collector.singleResult
  }

  /**
    * Called when a document is updated with new endTime
    */
  def startTimeFromTsIds(tsIds: Iterator[Int]): debox.Map[Int, Long] = {

    val startExecute = System.currentTimeMillis()
    val span = Kamon.currentSpan()
    val collector = new TsIdStartTimeCollector()
    val terms = new util.ArrayList[BytesRef]()
    tsIds.foreach { pId =>
      terms.add(new BytesRef(pId.toString.getBytes(StandardCharsets.UTF_8)))
    }
    // dont use BooleanQuery which will hit the 1024 term limit. Instead use TermInSetQuery which is
    // more efficient within Lucene
    withNewSearcher(s => s.search(new TermInSetQuery(TS_ID, terms), collector))
    span.tag(s"num-partitions-to-page", terms.size())
    val latency = System.currentTimeMillis - startExecute
    span.mark(s"index-startTimes-for-odp-lookup-latency=${latency}ms")
    startTimeLookupLatency.record(latency)
    collector.startTimes
  }

  /**
    * Called when a document is updated with new endTime
    */
  def endTimeFromTsId(tsId: Int): Long = {
    val collector = new NumericDocValueCollector(TsKeyLuceneIndex.END_TIME)
    withNewSearcher(s => s.search(new TermQuery(new Term(TS_ID, tsId.toString)), collector))
    collector.singleResult
  }

  /**
    * Query top-k tsIds matching a range of endTimes, in ascending order of endTime
    *
    * Note: This uses a collector that uses a PriorityQueue underneath covers.
    * O(k) heap memory will be used.
    */
  def tsIdsOrderedByEndTime(topk: Int,
                            fromEndTime: Long = 0,
                            toEndTime: Long = Long.MaxValue): EWAHCompressedBitmap = {
    val coll = new TopKTsIdsCollector(topk)
    withNewSearcher(s => s.search(LongPoint.newRangeQuery(END_TIME, fromEndTime, toEndTime), coll))
    coll.topKTsIdsBitmap()
  }

  def foreachTsKeyStillIngesting(func: (Int, BytesRef) => Unit): Int = {
    val coll = new ActionCollector(func)
    withNewSearcher(s => s.search(LongPoint.newExactQuery(END_TIME, Long.MaxValue), coll))
    coll.numHits
  }

  def updateTsKeyWithEndTime(tsKeyOnHeapBytes: Array[Byte],
                             tsId: Int,
                             endTime: Long = Long.MaxValue,
                             tsKeyBytesRefOffset: Int = 0)
                            (tsKeyNumBytes: Int = tsKeyOnHeapBytes.length): Unit = {
    var startTime = startTimeFromTsId(tsId) // look up index for old start time
    if (startTime == NOT_FOUND) {
      startTime = System.currentTimeMillis() - retentionMillis
      logger.warn(s"Could not find in Lucene startTime for tsId=$tsId in dataset=$ref. Using " +
        s"$startTime instead.", new IllegalStateException()) // assume this time series started retention period ago
    }
    val updatedDoc = makeDocument(tsKeyOnHeapBytes, tsKeyBytesRefOffset, tsKeyNumBytes,
      tsId, startTime, endTime)
    logger.debug(s"Updating document ${tsKeyString(tsId, tsKeyOnHeapBytes, tsKeyBytesRefOffset)} " +
                 s"with startTime=$startTime endTime=$endTime into dataset=$ref shard=$shardNum")
    indexWriter.updateDocument(new Term(TS_ID, tsId.toString), updatedDoc)
  }

  /**
    * Refresh readers with updates to index. May be expensive - use carefully.
    * @return
    */
  def refreshReadersBlocking(): Unit = {
    searcherManager.maybeRefreshBlocking()
    logger.info(s"Refreshed index searchers to make reads consistent for dataset=$ref shard=$shardNum")
  }

  private def leafFilter(column: String, filter: Filter): Query = {
    filter match {
      case EqualsRegex(value) =>
        val term = new Term(column, value.toString)
        new RegexpQuery(term, RegExp.NONE)
      case NotEqualsRegex(value) =>
        val term = new Term(column, value.toString)
        val allDocs = new MatchAllDocsQuery
        val booleanQuery = new BooleanQuery.Builder
        booleanQuery.add(allDocs, Occur.FILTER)
        booleanQuery.add(new RegexpQuery(term, RegExp.NONE), Occur.MUST_NOT)
        booleanQuery.build()
      case Equals(value) =>
        val term = new Term(column, value.toString)
        new TermQuery(term)
      case NotEquals(value) =>
        val term = new Term(column, value.toString)
        val booleanQuery = new BooleanQuery.Builder
        val termAll = new Term(column, ".*")
        booleanQuery.add(new RegexpQuery(termAll, RegExp.NONE), Occur.FILTER)
        booleanQuery.add(new TermQuery(term), Occur.MUST_NOT)
        booleanQuery.build()
      case In(values) =>
        if (values.size < 2)
          throw new IllegalArgumentException("In filter should have atleast 2 values")
        val booleanQuery = new BooleanQuery.Builder
        values.foreach { value =>
          booleanQuery.add(new TermQuery(new Term(column, value.toString)), Occur.SHOULD)
        }
        booleanQuery.build()
      case And(lhs, rhs) =>
        val andQuery = new BooleanQuery.Builder
        andQuery.add(leafFilter(column, lhs), Occur.FILTER)
        andQuery.add(leafFilter(column, rhs), Occur.FILTER)
        andQuery.build()
      case _ => throw new UnsupportedOperationException
    }
  }

  def tsIdsFromFilters(columnFilters: Seq[ColumnFilter],
                       startTime: Long,
                       endTime: Long): debox.Buffer[Int] = {
    val collector = new TsIdCollector() // passing zero for unlimited results
    searchFromFilters(columnFilters, startTime, endTime, collector)
    collector.result
  }

  def tsKeyRecordsFromFilters(columnFilters: Seq[ColumnFilter],
                              startTime: Long,
                              endTime: Long): Seq[TsKeyLuceneIndexRecord] = {
    val collector = new TsKeyRecordCollector()
    searchFromFilters(columnFilters, startTime, endTime, collector)
    collector.records
  }

  private def searchFromFilters(columnFilters: Seq[ColumnFilter],
                         startTime: Long,
                         endTime: Long,
                         collector: Collector): Unit = {

    val startExecute = System.currentTimeMillis()
    val span = Kamon.currentSpan()
    val booleanQuery = new BooleanQuery.Builder
    columnFilters.foreach { filter =>
      val q = leafFilter(filter.column, filter.filter)
      booleanQuery.add(q, Occur.FILTER)
    }
    booleanQuery.add(LongPoint.newRangeQuery(START_TIME, 0, endTime), Occur.FILTER)
    booleanQuery.add(LongPoint.newRangeQuery(END_TIME, startTime, Long.MaxValue), Occur.FILTER)
    val query = booleanQuery.build()
    logger.debug(s"Querying dataset=$ref shard=$shardNum tsKeyIndex with: $query")
    withNewSearcher(s => s.search(query, collector))
    val latency = System.currentTimeMillis - startExecute
    span.mark(s"index-partition-lookup-latency=${latency}ms")
    queryIndexLookupLatency.record(latency)
  }

  def tsIdFromTsKeySlow(tsKeyBase: Any,
                        tsKeyOffset: Long): Option[Int] = {

    val columnFilters = schema.binSchema.toStringPairs(tsKeyBase, tsKeyOffset)
      .map { pair => ColumnFilter(pair._1, Filter.Equals(pair._2)) }

    val startExecute = System.currentTimeMillis()
    val booleanQuery = new BooleanQuery.Builder
    columnFilters.foreach { filter =>
      val q = leafFilter(filter.column, filter.filter)
      booleanQuery.add(q, Occur.FILTER)
    }
    val query = booleanQuery.build()
    logger.debug(s"Querying dataset=$ref shard=$shardNum tsKeyIndex with: $query")
    var chosenTsId: Option[Int] = None
    def handleMatch(tsId: Int, candidate: BytesRef): Unit = {
      // we need an equals check because there can potentially be another tsKey with additional tags
      if (schema.binSchema.equals(tsKeyBase, tsKeyOffset,
        candidate.bytes, TsKeyLuceneIndex.bytesRefToUnsafeOffset(candidate.offset))) {
        logger.debug(s"There is already a tsId=$tsId assigned for " +
          s"${schema.binSchema.stringify(tsKeyBase, tsKeyOffset)} in" +
          s" dataset=$ref shard=$shardNum")
        chosenTsId = chosenTsId.orElse(Some(tsId))
      }
    }
    val collector = new ActionCollector(handleMatch)
    withNewSearcher(s => s.search(query, collector))
    tsIdFromTsKeyLookupLatency.record(Math.max(0, System.currentTimeMillis - startExecute))
    chosenTsId
  }
}

class NumericDocValueCollector(docValueName: String) extends SimpleCollector {

  var docValue: NumericDocValues = _
  var singleResult: Long = TsKeyLuceneIndex.NOT_FOUND

  // gets called for each segment
  override def doSetNextReader(context: LeafReaderContext): Unit = {
    docValue = context.reader().getNumericDocValues(docValueName)
  }

  // gets called for each matching document in current segment
  override def collect(doc: Int): Unit = {
    if (docValue.advanceExact(doc)) {
      singleResult = docValue.longValue()
    } else {
      throw new IllegalStateException("This shouldn't happen since every document should have a docValue")
    }
  }

  override def needsScores(): Boolean = false
}

class SingleTsKeyCollector extends SimpleCollector {

  var tsKeyDv: BinaryDocValues = _
  var singleResult: BytesRef = _

  // gets called for each segment
  override def doSetNextReader(context: LeafReaderContext): Unit = {
    tsKeyDv = context.reader().getBinaryDocValues(TsKeyLuceneIndex.TS_KEY)
  }

  // gets called for each matching document in current segment
  override def collect(doc: Int): Unit = {
    if (tsKeyDv.advanceExact(doc)) {
      singleResult = tsKeyDv.binaryValue()
    } else {
      throw new IllegalStateException("This shouldn't happen since every document should have a tsKeyDv")
    }
  }

  override def needsScores(): Boolean = false
}

class SingleTsIdCollector extends SimpleCollector {

  var tsIdDv: NumericDocValues = _
  var singleResult: Int = TsKeyLuceneIndex.NOT_FOUND

  // gets called for each segment
  override def doSetNextReader(context: LeafReaderContext): Unit = {
    tsIdDv = context.reader().getNumericDocValues(TsKeyLuceneIndex.TS_ID)
  }

  // gets called for each matching document in current segment
  override def collect(doc: Int): Unit = {
    if (tsIdDv.advanceExact(doc)) {
      singleResult = tsIdDv.longValue().toInt
    } else {
      throw new IllegalStateException("This shouldn't happen since every document should have a tsKeyDv")
    }
  }

  override def needsScores(): Boolean = false
}

/**
  * A collector that takes advantage of index sorting within segments
  * to collect the top-k results matching the query.
  *
  * It uses a priority queue of size k across each segment. It early terminates
  * a segment once k elements have been added, or if all smaller values in the
  * segment have been examined.
  */
class TopKTsIdsCollector(limit: Int) extends Collector with StrictLogging {

  import TsKeyLuceneIndex._

  var endTimeDv: NumericDocValues = _
  var tsIdDv: NumericDocValues = _
  val endTimeComparator = Ordering.by[(Int, Long), Long](_._2).reverse
  val topkResults = new PriorityQueue[(Int, Long)](limit, endTimeComparator)

  // gets called for each segment; need to return collector for that segment
  def getLeafCollector(context: LeafReaderContext): LeafCollector = {
    logger.trace("New segment inspected:" + context.id)
    endTimeDv = DocValues.getNumeric(context.reader, END_TIME)
    tsIdDv = DocValues.getNumeric(context.reader, TS_ID)

    new LeafCollector() {
      def setScorer(scorer: Scorer): Unit = {}

      // gets called for each matching document in the segment.
      def collect(doc: Int): Unit = {
        val tsIdValue = if (tsIdDv.advanceExact(doc)) {
          tsIdDv.longValue().toInt
        } else throw new IllegalStateException("This shouldn't happen since every document should have a tsId")
        if (endTimeDv.advanceExact(doc)) {
          val endTimeValue = endTimeDv.longValue
          if (topkResults.size < limit) {
            topkResults.add((tsIdValue, endTimeValue))
          }
          else if (topkResults.peek._2 > endTimeValue) {
            topkResults.remove()
            topkResults.add((tsIdValue, endTimeValue))
          }
          else { // terminate further iteration on current segment by throwing this exception
            throw new CollectionTerminatedException
          }
        } else throw new IllegalStateException("This shouldn't happen since every document should have an endTime")
      }
    }
  }

  def needsScores(): Boolean = false

  def topKTsIds(): IntIterator = {
    val result = new EWAHCompressedBitmap()
    topkResults.iterator().asScala.foreach { p => result.set(p._1) }
    result.intIterator()
  }

  def topKTsIdsBitmap(): EWAHCompressedBitmap = {
    val result = new EWAHCompressedBitmap()
    topkResults.iterator().asScala.foreach { p => result.set(p._1) }
    result
  }
}

class TsIdCollector extends SimpleCollector {
  val result: debox.Buffer[Int] = debox.Buffer.empty[Int]
  private var tsIdDv: NumericDocValues = _

  override def needsScores(): Boolean = false

  override def doSetNextReader(context: LeafReaderContext): Unit = {
    //set the subarray of the numeric values for all documents in the context
    tsIdDv = context.reader().getNumericDocValues(TsKeyLuceneIndex.TS_ID)
  }

  override def collect(doc: Int): Unit = {
    if (tsIdDv.advanceExact(doc)) {
      result += tsIdDv.longValue().toInt
    } else {
      throw new IllegalStateException("This shouldn't happen since every document should have a tsIdDv")
    }
  }
}

class TsIdStartTimeCollector extends SimpleCollector {
  val startTimes = debox.Map.empty[Int, Long]
  private var tsIdDv: NumericDocValues = _
  private var startTimeDv: NumericDocValues = _

  override def needsScores(): Boolean = false

  override def doSetNextReader(context: LeafReaderContext): Unit = {
    //set the subarray of the numeric values for all documents in the context
    tsIdDv = context.reader().getNumericDocValues(TsKeyLuceneIndex.TS_ID)
    startTimeDv = context.reader().getNumericDocValues(TsKeyLuceneIndex.START_TIME)
  }

  override def collect(doc: Int): Unit = {
    if (tsIdDv.advanceExact(doc) && startTimeDv.advanceExact(doc)) {
      val tsd = tsIdDv.longValue().toInt
      val startTime = startTimeDv.longValue()
      startTimes(tsd) = startTime
    } else {
      throw new IllegalStateException("This shouldn't happen since every document should have tsIdDv and startTimeDv")
    }
  }
}

class TsKeyRecordCollector extends SimpleCollector {
  val records = new ArrayBuffer[TsKeyLuceneIndexRecord]
  private var tsKeyDv: BinaryDocValues = _
  private var startTimeDv: NumericDocValues = _
  private var endTimeDv: NumericDocValues = _

  override def needsScores(): Boolean = false

  override def doSetNextReader(context: LeafReaderContext): Unit = {
    tsKeyDv = context.reader().getBinaryDocValues(TsKeyLuceneIndex.TS_KEY)
    startTimeDv = context.reader().getNumericDocValues(TsKeyLuceneIndex.START_TIME)
    endTimeDv = context.reader().getNumericDocValues(TsKeyLuceneIndex.END_TIME)
  }

  override def collect(doc: Int): Unit = {
    if (tsKeyDv.advanceExact(doc) && startTimeDv.advanceExact(doc) && endTimeDv.advanceExact(doc)) {
      val pkBytesRef = tsKeyDv.binaryValue()
      // Gotcha! make copy of array because lucene reuses bytesRef for next result
      val pkBytes = util.Arrays.copyOfRange(pkBytesRef.bytes, pkBytesRef.offset, pkBytesRef.offset + pkBytesRef.length)
      records += TsKeyLuceneIndexRecord(pkBytes, startTimeDv.longValue(), endTimeDv.longValue())
    } else {
      throw new IllegalStateException("This shouldn't happen since every document should have tsIdDv and startTimeDv")
    }
  }
}

class ActionCollector(action: (Int, BytesRef) => Unit) extends SimpleCollector {
  private var tsIdDv: NumericDocValues = _
  private var tsKeyDv: BinaryDocValues = _
  private var counter: Int = 0

  override def needsScores(): Boolean = false

  override def doSetNextReader(context: LeafReaderContext): Unit = {
    tsIdDv = context.reader().getNumericDocValues(TsKeyLuceneIndex.TS_ID)
    tsKeyDv = context.reader().getBinaryDocValues(TsKeyLuceneIndex.TS_KEY)
  }

  override def collect(doc: Int): Unit = {
    if (tsIdDv.advanceExact(doc) && tsKeyDv.advanceExact(doc)) {
      val tsId = tsIdDv.longValue().toInt
      val tsKey = tsKeyDv.binaryValue()
      action(tsId, tsKey)
      counter += 1
    } else {
      throw new IllegalStateException("This shouldn't happen since every document should have a tsIdDv && tsKeyDv")
    }
  }

  def numHits: Int = counter
}

class LuceneMetricsRouter(ref: DatasetRef, shard: Int) extends InfoStream with StrictLogging {
  override def message(component: String, message: String): Unit = {
    logger.debug(s"dataset=$ref shard=$shard component=$component $message")
    // TODO parse string and report metrics to kamon
  }
  override def isEnabled(component: String): Boolean = true
  override def close(): Unit = {}
}
