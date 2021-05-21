package filodb.core.store

import com.typesafe.scalalogging.StrictLogging

import filodb.core.query._
import filodb.memory.BinaryRegionLarge

sealed trait TimeseriesScanMethod {
  def shard: Int
}

final case class SingleTimeseriesScan(partition: Array[Byte], shard: Int = 0) extends TimeseriesScanMethod

object SingleTimeseriesScan {
  def apply(partKeyAddr: Long, shard: Int): SingleTimeseriesScan =
    SingleTimeseriesScan(BinaryRegionLarge.asNewByteArray(partKeyAddr), shard)
}

final case class MultiTimeseriesScan(partitions: Seq[Array[Byte]],
                                     shard: Int = 0) extends TimeseriesScanMethod
// NOTE: One ColumnFilter per column please.
final case class FilteredTimeseriesScan(split: ScanSplit,
                                        filters: Seq[ColumnFilter] = Nil) extends TimeseriesScanMethod {
  def shard: Int = split match {
    case ShardSplit(shard) => shard
    case other: ScanSplit  => ???
  }
}

sealed trait ChunkScanMethod {
  def startTime: Long
  def endTime: Long
}

trait AllTimeScanMethod {
  def startTime: Long = Long.MinValue
  def endTime: Long = Long.MaxValue
}

case object AllChunkScan extends AllTimeScanMethod with ChunkScanMethod
final case class TimeRangeChunkScan(startTime: Long, endTime: Long) extends ChunkScanMethod
case object WriteBufferChunkScan extends AllTimeScanMethod with ChunkScanMethod
// Only read chunks which are in memory
case object InMemoryChunkScan extends AllTimeScanMethod with ChunkScanMethod

trait ScanSplit {
  // Should return a set of hostnames or IP addresses describing the preferred hosts for that scan split
  def hostnames: Set[String]
}

final case class ShardSplit(shard: Int) extends ScanSplit {
  def hostnames: Set[String] = Set.empty
}

/**
 * ColumnStore defines all of the read/query methods for a ColumnStore.
 * TODO: only here to keep up appearances with old stuff, refactor further.
 */
trait ColumnStore extends ChunkSink with RawChunkSource with StrictLogging {
  /**
   * Shuts down the ColumnStore, including any threads that might be hanging around
   */
  def shutdown(): Unit
}
