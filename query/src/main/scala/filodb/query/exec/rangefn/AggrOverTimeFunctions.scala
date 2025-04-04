package filodb.query.exec.rangefn

import java.lang.{Double => JLDouble}
import java.util

import debox.Buffer

import filodb.core.query.{QueryConfig, TransientHistMaxMinRow, TransientHistRow, TransientRow}
import filodb.core.store.ChunkSetInfoReader
import filodb.memory.format.{BinaryVector, MemoryReader, VectorDataReader}
import filodb.memory.format.{vectors => bv}
import filodb.memory.format.vectors.DoubleIterator
import filodb.query.exec.{FuncArgs, StaticFuncArgs}

class MinMaxOverTimeFunction(ord: Ordering[Double]) extends RangeFunction {
  val minMaxDeque = new util.ArrayDeque[TransientRow]()

  override def addedToWindow(row: TransientRow, window: Window): Unit = {
    if (!row.value.isNaN) {
      while (!minMaxDeque.isEmpty && ord.compare(minMaxDeque.peekLast().value, row.value) < 0) minMaxDeque.removeLast()
      minMaxDeque.addLast(row)
    }
  }

  override def removedFromWindow(row: TransientRow, window: Window): Unit = {
    while (!minMaxDeque.isEmpty && minMaxDeque.peekFirst().timestamp <= row.timestamp) minMaxDeque.removeFirst()
  }

  override def apply(startTimestamp: Long, endTimestamp: Long, window: Window,
                     sampleToEmit: TransientRow,
                     queryConfig: QueryConfig): Unit = {
    if (minMaxDeque.isEmpty) sampleToEmit.setValues(endTimestamp, Double.NaN)
    else sampleToEmit.setValues(endTimestamp, minMaxDeque.peekFirst().value)
  }
}

class MinOverTimeChunkedFunctionD(var min: Double = Double.NaN) extends ChunkedDoubleRangeFunction {
  override final def reset(): Unit = { min = Double.NaN }
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    sampleToEmit.setValues(endTimestamp, min)
  }
  final def addTimeDoubleChunks(doubleVectAcc: MemoryReader,
                                doubleVect: BinaryVector.BinaryVectorPtr,
                                doubleReader: bv.DoubleVectorDataReader,
                                startRowNum: Int,
                                endRowNum: Int): Unit = {
    var rowNum = startRowNum
    val it = doubleReader.iterate(doubleVectAcc, doubleVect, startRowNum)
    while (rowNum <= endRowNum) {
      val nextVal = it.next
      min = if (min.isNaN) nextVal else Math.min(min, nextVal)
      rowNum += 1
    }
  }
}

class MinOverTimeChunkedFunctionL(var min: Long = Long.MaxValue) extends ChunkedLongRangeFunction {
  override final def reset(): Unit = { min = Long.MaxValue }
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    sampleToEmit.setValues(endTimestamp, min.toDouble)
  }
  final def addTimeLongChunks(longVectAcc: MemoryReader,
                              longVect: BinaryVector.BinaryVectorPtr,
                              longReader: bv.LongVectorDataReader,
                              startRowNum: Int,
                              endRowNum: Int): Unit = {
    var rowNum = startRowNum
    val it = longReader.iterate(longVectAcc, longVect, startRowNum)
    while (rowNum <= endRowNum) {
      min = Math.min(min, it.next)
      rowNum += 1
    }
  }
}

class MaxOverTimeChunkedFunctionD(var max: Double = Double.NaN) extends ChunkedDoubleRangeFunction {
  override final def reset(): Unit = { max = Double.NaN }
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    sampleToEmit.setValues(endTimestamp, max)
  }
  final def addTimeDoubleChunks(doubleVectAcc: MemoryReader,
                                doubleVect: BinaryVector.BinaryVectorPtr,
                                doubleReader: bv.DoubleVectorDataReader,
                                startRowNum: Int,
                                endRowNum: Int): Unit = {
    var rowNum = startRowNum
    val it = doubleReader.iterate(doubleVectAcc, doubleVect, startRowNum)
    while (rowNum <= endRowNum) {
      val nextVal = it.next
      max = if (max.isNaN) nextVal else Math.max(max, nextVal) // cannot compare NaN, always < anything else
      rowNum += 1
    }
  }
}

class MaxOverTimeChunkedFunctionL(var max: Long = Long.MinValue) extends ChunkedLongRangeFunction {
  override final def reset(): Unit = { max = Long.MinValue }
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    sampleToEmit.setValues(endTimestamp, max.toDouble)
  }
  final def addTimeLongChunks(longVectAcc: MemoryReader,
                              longVect: BinaryVector.BinaryVectorPtr,
                              longReader: bv.LongVectorDataReader,
                              startRowNum: Int,
                              endRowNum: Int): Unit = {
    var rowNum = startRowNum
    val it = longReader.iterate(longVectAcc, longVect, startRowNum)
    while (rowNum <= endRowNum) {
      max = Math.max(max, it.next)
      rowNum += 1
    }
  }
}

class SumOverTimeFunction(var sum: Double = Double.NaN, var count: Int = 0) extends RangeFunction {
  override def addedToWindow(row: TransientRow, window: Window): Unit = {
    if (!JLDouble.isNaN(row.value)) {
      if (sum.isNaN) {
        sum = 0d
      }
      sum += row.value
      count += 1
    }
  }

  override def removedFromWindow(row: TransientRow, window: Window): Unit = {
    if (!JLDouble.isNaN(row.value)) {
      if (sum.isNaN) {
        sum = 0d
      }
      sum -= row.value
      count -= 1
      if (count == 0) { // There is no value in window
        sum = Double.NaN
      }
    }
  }

  override def apply(startTimestamp: Long, endTimestamp: Long, window: Window,
                     sampleToEmit: TransientRow,
                     queryConfig: QueryConfig): Unit = {
    sampleToEmit.setValues(endTimestamp, sum)
  }
}

object ChangesOverTimeFunction extends RangeFunction {
  override def addedToWindow(row: TransientRow, window: Window): Unit = {
  }

  override def removedFromWindow(row: TransientRow, window: Window): Unit = {
  }

  override def apply(
    startTimestamp: Long, endTimestamp: Long, window: Window,
    sampleToEmit: TransientRow,
    queryConfig: QueryConfig
  ): Unit = {
    var lastValue = Double.NaN
    var changes = Double.NaN
    if (window.size > 0) lastValue = window.head.getDouble(1)
    if (!lastValue.isNaN) {
      changes = 0
    }
    var i = 1;
    while (i < window.size) {
      val curValue = window.apply(i).getDouble(1)
      if (!curValue.isNaN && !lastValue.isNaN) {
        if (curValue != lastValue) {
            changes = changes + 1
        }
      }
      if (!curValue.isNaN) {
        lastValue = curValue
        if (changes.isNaN) {
          changes = 0
        }
      }
      i = i + 1
    }
    sampleToEmit.setValues(endTimestamp, changes)
  }
}


object QuantileOverTimeFunction {
  def calculateRank(q: Double, counter: Int): (Double, Int, Int) = {
    val rank = q*(counter - 1)
    val lowerIndex = Math.max(0, Math.floor(rank))
    val upperIndex = Math.min(counter - 1, lowerIndex + 1)
    val weight = rank - math.floor(rank)
    (weight, upperIndex.toInt, lowerIndex.toInt)
  }
}

class QuantileOverTimeFunction(funcParams: Seq[Any]) extends RangeFunction {
  override def addedToWindow(row: TransientRow, window: Window): Unit = {
  }

  override def removedFromWindow(row: TransientRow, window: Window): Unit = {
  }

  override def apply(
    startTimestamp: Long, endTimestamp: Long, window: Window,
    sampleToEmit: TransientRow,
    queryConfig: QueryConfig
  ): Unit = {
    require(funcParams.head.isInstanceOf[StaticFuncArgs], "quantile parameter must be a number")
    val q = funcParams.head.asInstanceOf[StaticFuncArgs].scalar

    val values: Buffer[Double] = Buffer.ofSize(window.size)
    var i = 0;
    while (i < window.size) {
      val curValue = window.apply(i).getDouble(1)
      if (!curValue.isNaN) {
        values.append(curValue)
      }
      i = i + 1
    }
    val counter = values.length
    values.sort(spire.algebra.Order.fromOrdering[Double])
    val (weight, upperIndex, lowerIndex) = QuantileOverTimeFunction.calculateRank(q, counter)
    var quantileResult : Double = Double.NaN
    if (counter > 0) {
      quantileResult = values(lowerIndex)*(1-weight) + values(upperIndex)*weight
    }
    sampleToEmit.setValues(endTimestamp, quantileResult)

  }
}

class MedianAbsoluteDeviationOverTimeFunction(funcParams: Seq[Any]) extends RangeFunction {
  override def addedToWindow(row: TransientRow, window: Window): Unit = {
  }

  override def removedFromWindow(row: TransientRow, window: Window): Unit = {
  }

  override def apply(
                      startTimestamp: Long, endTimestamp: Long, window: Window,
                      sampleToEmit: TransientRow,
                      queryConfig: QueryConfig
                    ): Unit = {
    val q = 0.5
    val values: Buffer[Double] = Buffer.ofSize(window.size)
    for (i <- 0 until window.size) {
      val curValue = window.apply(i).getDouble(1)
      if (!curValue.isNaN) {
        values.append(curValue)
      }
    }
    val size = values.length
    values.sort(spire.algebra.Order.fromOrdering[Double])
    val (weight, upperIndex, lowerIndex) = QuantileOverTimeFunction.calculateRank(q, size)
    var median : Double = Double.NaN
    if (size > 0) {
      median = values(lowerIndex)*(1-weight) + values(upperIndex)*weight
    }

    var medianAbsoluteDeviationResult : Double = Double.NaN
    val diffFromMedians: Buffer[Double] = Buffer.ofSize(window.size)

    for (i <- 0 until window.size) {
      val curValue = window.apply(i).getDouble(1)
      diffFromMedians.append(Math.abs(median - curValue))
    }
    diffFromMedians.sort(spire.algebra.Order.fromOrdering[Double])
    if (size > 0) {
      medianAbsoluteDeviationResult = diffFromMedians(lowerIndex)*(1-weight) + diffFromMedians(upperIndex)*weight
    }
    sampleToEmit.setValues(endTimestamp, medianAbsoluteDeviationResult)
  }
}

class LastOverTimeIsMadOutlierFunction(funcParams: Seq[Any]) extends RangeFunction {
  require(funcParams.size == 2, "last_over_time_is_mad_outlier function needs a two scalar arguments" +
                  " (tolerance, bounds)")
  require(funcParams(0).isInstanceOf[StaticFuncArgs], "first tolerance parameter must be a number; " +
    "higher value means more tolerance to anomalies")
  require(funcParams(1).isInstanceOf[StaticFuncArgs], "second bounds parameter must be a number(0, 1, 2)")

  private val boundsCheck = funcParams(1).asInstanceOf[StaticFuncArgs].scalar.toInt
  require(boundsCheck == 0 || boundsCheck == 1 || boundsCheck == 2,
    "boundsCheck parameter should be 0 (lower only), 1 (both lower and upper) or 2 (upper only)")

  private val tolerance = funcParams.head.asInstanceOf[StaticFuncArgs].scalar
  require(tolerance > 0, "tolerance must be a positive number")

  override def addedToWindow(row: TransientRow, window: Window): Unit = {
  }

  override def removedFromWindow(row: TransientRow, window: Window): Unit = {
  }

  override def apply(startTimestamp: Long, endTimestamp: Long, window: Window,
                      sampleToEmit: TransientRow,
                      queryConfig: QueryConfig
                    ): Unit = {
    val size = window.size
    if (size > 0) {
      // find median
      val q = 0.5
      val values: Buffer[Double] = Buffer.ofSize(size)
      for (i <- 0 until size) {
        val curValue = window.apply(i).getDouble(1)
        if (!curValue.isNaN) {
          values.append(curValue)
        }
      }
      values.sort(spire.algebra.Order.fromOrdering[Double])
      val (weight, upperIndex, lowerIndex) = QuantileOverTimeFunction.calculateRank(q, size)
      val median = values(lowerIndex)*(1-weight) + values(upperIndex)*weight

      // distance from median
      val distFromMedian: Buffer[Double] = Buffer.ofSize(size)
      for (i <- 0 until size) {
        val curValue = window.apply(i).getDouble(1)
        distFromMedian.append(Math.abs(median - curValue))
      }

      // mad = median of absolute distances from median
      distFromMedian.sort(spire.algebra.Order.fromOrdering[Double])
      val mad = distFromMedian(lowerIndex)*(1-weight) + distFromMedian(upperIndex)*weight

      // classify last point as anomaly if it's more than `tolerance * mad` away from median
      val lowerBound = median - tolerance * mad
      val upperBound = median + tolerance * mad
      val lastValue = window.last.getDouble(1)
      if ((lastValue < lowerBound && boundsCheck <= 1) || (lastValue > upperBound && boundsCheck >= 1)) {
        sampleToEmit.setValues(endTimestamp, lastValue)
      } else {
        sampleToEmit.setValues(endTimestamp, Double.NaN)
      }
    } else {
      sampleToEmit.setValues(endTimestamp, Double.NaN)
    }
  }
}

abstract class SumOverTimeChunkedFunction(var sum: Double = Double.NaN) extends ChunkedRangeFunction[TransientRow] {
  override final def reset(): Unit = { sum = Double.NaN }
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    sampleToEmit.setValues(endTimestamp, sum)
  }
}

class SumOverTimeChunkedFunctionD extends SumOverTimeChunkedFunction() with ChunkedDoubleRangeFunction {
  final def addTimeDoubleChunks(doubleVectAcc: MemoryReader,
                                doubleVect: BinaryVector.BinaryVectorPtr,
                                doubleReader: bv.DoubleVectorDataReader,
                                startRowNum: Int,
                                endRowNum: Int): Unit = {

    //Takes care of computing sum in all seq types : combination of NaN & not NaN & all numbers.
    val chunkSum = doubleReader.sum(doubleVectAcc, doubleVect, startRowNum, endRowNum)
    if(!JLDouble.isNaN(chunkSum) && JLDouble.isNaN(sum)) sum = 0d
    sum += chunkSum
  }
}

class SumOverTimeChunkedFunctionL extends SumOverTimeChunkedFunction() with ChunkedLongRangeFunction {
  final def addTimeLongChunks(longVectAcc: MemoryReader,
                              longVect: BinaryVector.BinaryVectorPtr,
                              longReader: bv.LongVectorDataReader,
                              startRowNum: Int,
                              endRowNum: Int): Unit = {
    if (sum.isNaN) {
      sum = 0d
    }
    sum += longReader.sum(longVectAcc, longVect, startRowNum, endRowNum)
  }
}

class SumOverTimeChunkedFunctionH(var h: bv.MutableHistogram = bv.Histogram.empty)
extends TimeRangeFunction[TransientHistRow] {
  override final def reset(): Unit = { h = bv.Histogram.empty }
  final def apply(endTimestamp: Long, sampleToEmit: TransientHistRow): Unit = {
    sampleToEmit.setValues(endTimestamp, h)
  }

  final def addTimeChunks(vectAcc: MemoryReader,
                          vectPtr: BinaryVector.BinaryVectorPtr,
                          reader: VectorDataReader,
                          startRowNum: Int,
                          endRowNum: Int): Unit = {
    val sum = reader.asHistReader.sum(startRowNum, endRowNum)
    h match {
      // sum is mutable histogram, copy to be sure it's our own copy
      case hist if hist.numBuckets == 0 => h = sum.copy
      case hist: bv.MutableHistogram    => hist.add(sum)
    }
  }
}

/**
 * Sums Histograms over time and also computes Max over time of a Max field.
 * @param maxColID the data column ID containing the max column
 */
class SumAndMaxOverTimeFuncHD(maxColID: Int) extends ChunkedRangeFunction[TransientHistMaxMinRow] {
  private val hFunc = new SumOverTimeChunkedFunctionH
  private val maxFunc = new MaxOverTimeChunkedFunctionD

  override final def reset(): Unit = {
    hFunc.reset()
    maxFunc.reset()
  }
  final def apply(endTimestamp: Long, sampleToEmit: TransientHistMaxMinRow): Unit = {
    sampleToEmit.setValues(endTimestamp, hFunc.h)
    sampleToEmit.setDouble(2, maxFunc.max)
  }

  import BinaryVector.BinaryVectorPtr

  // scalastyle:off parameter.number
  def addChunks(tsVectorAcc: MemoryReader, tsVector: BinaryVectorPtr, tsReader: bv.LongVectorDataReader,
                valueVectorAcc: MemoryReader, valueVector: BinaryVectorPtr, valueReader: VectorDataReader,
                startTime: Long, endTime: Long, info: ChunkSetInfoReader, queryConfig: QueryConfig): Unit = {
    // Do BinarySearch for start/end pos only once for both columns == WIN!
    val startRowNum = tsReader.binarySearch(tsVectorAcc, tsVector, startTime) & 0x7fffffff
    val endRowNum = Math.min(tsReader.ceilingIndex(tsVectorAcc, tsVector, endTime), info.numRows - 1)

    // At least one sample is present
    if (startRowNum <= endRowNum) {
      hFunc.addTimeChunks(valueVectorAcc, valueVector, valueReader, startRowNum, endRowNum)

      // Get valueVector/reader for max column
      val maxVectAcc = info.vectorAccessor(maxColID)
      val maxVectPtr = info.vectorAddress(maxColID)
      maxFunc.addTimeChunks(maxVectAcc, maxVectPtr, bv.DoubleVector(maxVectAcc, maxVectPtr), startRowNum, endRowNum)
    }
  }
}

class RateAndMinMaxOverTimeFuncHD(maxColId: Int, minColId: Int) extends ChunkedRangeFunction[TransientHistMaxMinRow] {
  private val hFunc = new RateOverDeltaChunkedFunctionH
  private val maxFunc = new MaxOverTimeChunkedFunctionD
  private val minFunc = new MinOverTimeChunkedFunctionD

  override final def reset(): Unit = {
    hFunc.reset()
    maxFunc.reset()
    minFunc.reset()
  }

  override def apply(windowStart: Long, windowEnd: Long, sampleToEmit: TransientHistMaxMinRow): Unit = {
    hFunc.apply(windowStart, windowEnd, sampleToEmit)
    sampleToEmit.setDouble(2, maxFunc.max)
    sampleToEmit.setDouble(3, minFunc.min)
  }
  final def apply(endTimestamp: Long, sampleToEmit: TransientHistMaxMinRow): Unit = ??? // should not be invoked

  import BinaryVector.BinaryVectorPtr

  // scalastyle:off parameter.number
  def addChunks(tsVectorAcc: MemoryReader, tsVector: BinaryVectorPtr, tsReader: bv.LongVectorDataReader,
                valueVectorAcc: MemoryReader, valueVector: BinaryVectorPtr, valueReader: VectorDataReader,
                startTime: Long, endTime: Long, info: ChunkSetInfoReader, queryConfig: QueryConfig): Unit = {
    // Do BinarySearch for start/end pos only once for all columns == WIN!
    val startRowNum = tsReader.binarySearch(tsVectorAcc, tsVector, startTime) & 0x7fffffff
    val endRowNum = Math.min(tsReader.ceilingIndex(tsVectorAcc, tsVector, endTime), info.numRows - 1)

    // At least one sample is present
    if (startRowNum <= endRowNum) {
      hFunc.addTimeChunks(valueVectorAcc, valueVector, valueReader, startRowNum, endRowNum)

      // Get valueVector/reader for max column
      val maxVectAcc = info.vectorAccessor(maxColId)
      val maxVectPtr = info.vectorAddress(maxColId)
      maxFunc.addTimeChunks(maxVectAcc, maxVectPtr, bv.DoubleVector(maxVectAcc, maxVectPtr), startRowNum, endRowNum)

      // Get valueVector/reader for min column
      val minVectAcc = info.vectorAccessor(minColId)
      val minVectPtr = info.vectorAddress(minColId)
      minFunc.addTimeChunks(minVectAcc, minVectPtr, bv.DoubleVector(minVectAcc, minVectPtr), startRowNum, endRowNum)
    }
  }
}


/**
  * Computes Average Over Time using sum and count columns.
  * Used in when calculating avg_over_time using downsampled data
  */
class AvgWithSumAndCountOverTimeFuncD(countColId: Int) extends ChunkedRangeFunction[TransientRow] {
  private val sumFunc = new SumOverTimeChunkedFunctionD
  private val countFunc = new SumOverTimeChunkedFunctionD

  override final def reset(): Unit = {
    sumFunc.reset()
    countFunc.reset()
  }

  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    sampleToEmit.setValues(endTimestamp, sumFunc.sum / countFunc.sum)
  }

  import BinaryVector.BinaryVectorPtr

  // scalastyle:off parameter.number
  def addChunks(tsVectorAcc: MemoryReader, tsVector: BinaryVectorPtr, tsReader: bv.LongVectorDataReader,
                valueVectorAcc: MemoryReader, valueVector: BinaryVectorPtr, valueReader: VectorDataReader,
                startTime: Long, endTime: Long, info: ChunkSetInfoReader, queryConfig: QueryConfig): Unit = {
    // Do BinarySearch for start/end pos only once for both columns == WIN!
    val startRowNum = tsReader.binarySearch(tsVectorAcc, tsVector, startTime) & 0x7fffffff
    val endRowNum = Math.min(tsReader.ceilingIndex(tsVectorAcc, tsVector, endTime), info.numRows - 1)

    // At least one sample is present
    if (startRowNum <= endRowNum) {
      sumFunc.addTimeChunks(valueVectorAcc, valueVector, valueReader, startRowNum, endRowNum)

      // Get valueVector/reader for count column
      val countVectAcc = info.vectorAccessor(countColId)
      val countVectPtr = info.vectorAddress(countColId)
      countFunc.addTimeChunks(countVectAcc, countVectPtr,
        bv.DoubleVector(countVectAcc, countVectPtr), startRowNum, endRowNum)
    }
  }
}

/**
  * Computes Average Over Time using sum and count columns.
  * Used in when calculating avg_over_time using downsampled data
  */
class AvgWithSumAndCountOverTimeFuncL(countColId: Int) extends ChunkedRangeFunction[TransientRow] {
  private val sumFunc = new SumOverTimeChunkedFunctionL
  private val countFunc = new CountOverTimeChunkedFunction

  override final def reset(): Unit = {
    sumFunc.reset()
    countFunc.reset()
  }

  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    sampleToEmit.setValues(endTimestamp, sumFunc.sum / countFunc.count)
  }

  import BinaryVector.BinaryVectorPtr

  // scalastyle:off parameter.number
  def addChunks(tsVectorAcc: MemoryReader, tsVector: BinaryVectorPtr, tsReader: bv.LongVectorDataReader,
                valueVectorAcc: MemoryReader, valueVector: BinaryVectorPtr, valueReader: VectorDataReader,
                startTime: Long, endTime: Long, info: ChunkSetInfoReader, queryConfig: QueryConfig): Unit = {
    // Do BinarySearch for start/end pos only once for both columns == WIN!
    val startRowNum = tsReader.binarySearch(tsVectorAcc, tsVector, startTime) & 0x7fffffff
    val endRowNum = Math.min(tsReader.ceilingIndex(tsVectorAcc, tsVector, endTime), info.numRows - 1)

    // At least one sample is present
    if (startRowNum <= endRowNum) {
      sumFunc.addTimeChunks(valueVectorAcc, valueVector, valueReader, startRowNum, endRowNum)

      // Get valueVector/reader for count column
      val cntVectAcc = info.vectorAccessor(countColId)
      val cntVectPtr = info.vectorAddress(countColId)
      countFunc.addTimeChunks(cntVectAcc, cntVectPtr, bv.DoubleVector(cntVectAcc, cntVectPtr), startRowNum, endRowNum)
    }
  }
}

class CountOverTimeFunction(var count: Double = Double.NaN) extends RangeFunction {
  override def addedToWindow(row: TransientRow, window: Window): Unit = {
    if (!JLDouble.isNaN(row.value)) {
      if (count.isNaN) {
        count = 0d
      }
      count += 1
    }
  }

  override def removedFromWindow(row: TransientRow, window: Window): Unit = {
    if (!JLDouble.isNaN(row.value)) {
      if (count.isNaN) {
        count = 0d
      }
      count -= 1
      if (count==0) { //Reset count as no sample is present
        count = Double.NaN
      }
    }
  }

  override def apply(startTimestamp: Long, endTimestamp: Long, window: Window,
                     sampleToEmit: TransientRow,
                     queryConfig: QueryConfig): Unit = {
    sampleToEmit.setValues(endTimestamp, count)
  }
}

class CountOverTimeChunkedFunction(var count: Int = 0) extends TimeRangeFunction[TransientRow] {
  override final def reset(): Unit = { count = 0 }
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    sampleToEmit.setValues(endTimestamp, count.toDouble)
  }

  def addTimeChunks(vectAcc: MemoryReader,
                    vectPtr: BinaryVector.BinaryVectorPtr,
                    reader: VectorDataReader,
                    startRowNum: Int,
                    endRowNum: Int): Unit = {
    val numRows = endRowNum - startRowNum + 1
    count += numRows
  }
}

// Special count_over_time chunked function for doubles needed to not count NaNs whih are used by
// Prometheus to mark end of a time series.
// TODO: handle end of time series a different, better way.  This function shouldn't be needed.
class CountOverTimeChunkedFunctionD(var count: Double = Double.NaN) extends ChunkedDoubleRangeFunction {
  override final def reset(): Unit = { count = Double.NaN }
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    sampleToEmit.setValues(endTimestamp, count)
  }
  final def addTimeDoubleChunks(doubleVectAcc: MemoryReader,
                                doubleVect: BinaryVector.BinaryVectorPtr,
                                doubleReader: bv.DoubleVectorDataReader,
                                startRowNum: Int,
                                endRowNum: Int): Unit = {
    if (count.isNaN) {
      count = 0d
    }
    count += doubleReader.count(doubleVectAcc, doubleVect, startRowNum, endRowNum)
  }
}

class AvgOverTimeFunction(var sum: Double = Double.NaN, var count: Int = 0) extends RangeFunction {
  override def addedToWindow(row: TransientRow, window: Window): Unit = {
    if (!JLDouble.isNaN(row.value)) {
      if (sum.isNaN) {
        sum = 0d;
      }
      sum += row.value
      count += 1
    }
  }

  override def removedFromWindow(row: TransientRow, window: Window): Unit = {
    if (!JLDouble.isNaN(row.value)) {
      if (sum.isNaN) {
        sum = 0d;
      }
      sum -= row.value
      count -= 1
    }
  }

  override def apply(startTimestamp: Long, endTimestamp: Long, window: Window,
                     sampleToEmit: TransientRow,
                     queryConfig: QueryConfig): Unit = {
    if (count == 0) {
      sampleToEmit.setValues(endTimestamp, Double.NaN)
    } else {
      sampleToEmit.setValues(endTimestamp, sum/count)
    }
  }
}

abstract class AvgOverTimeChunkedFunction(var sum: Double = Double.NaN, var count: Int = 0)
  extends ChunkedRangeFunction[TransientRow] {
  override final def reset(): Unit = {
    sum = Double.NaN
    count = 0
  }

  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    sampleToEmit.setValues(endTimestamp, if (count > 0) sum / count else if (sum.isNaN()) sum else 0d)
  }
}

class AvgOverTimeChunkedFunctionD extends AvgOverTimeChunkedFunction() with ChunkedDoubleRangeFunction {
  final def addTimeDoubleChunks(doubleVectAcc: MemoryReader,
                                doubleVect: BinaryVector.BinaryVectorPtr,
                                doubleReader: bv.DoubleVectorDataReader,
                                startRowNum: Int,
                                endRowNum: Int): Unit = {

    // Takes care of computing sum in all seq types : combination of NaN & not NaN & all numbers.
    val chunkSum = doubleReader.sum(doubleVectAcc, doubleVect, startRowNum, endRowNum)
    if(!JLDouble.isNaN(chunkSum) && JLDouble.isNaN(sum)) sum = 0d
    sum += chunkSum
    count += doubleReader.count(doubleVectAcc, doubleVect, startRowNum, endRowNum)
  }
}

class AvgOverTimeChunkedFunctionL extends AvgOverTimeChunkedFunction() with ChunkedLongRangeFunction {
  final def addTimeLongChunks(longVectAcc: MemoryReader,
                              longVect: BinaryVector.BinaryVectorPtr,
                              longReader: bv.LongVectorDataReader,
                              startRowNum: Int,
                              endRowNum: Int): Unit = {
    sum += longReader.sum(longVectAcc, longVect, startRowNum, endRowNum)
    count += (endRowNum - startRowNum + 1)
  }
}

class StdDevOverTimeFunction(var sum: Double = 0d,
                             var count: Int = 0,
                             var squaredSum: Double = 0d) extends RangeFunction {
  override def addedToWindow(row: TransientRow, window: Window): Unit = {
    if (!row.value.isNaN) {
      sum += row.value
      squaredSum += row.value * row.value
      count += 1
    }
  }

  override def removedFromWindow(row: TransientRow, window: Window): Unit = {
    if (!row.value.isNaN) {
      sum -= row.value
      squaredSum -= row.value * row.value
      count -= 1
    }
  }

  override def apply(startTimestamp: Long, endTimestamp: Long, window: Window,
                     sampleToEmit: TransientRow,
                     queryConfig: QueryConfig): Unit = {
    val avg = sum/count
    val stdDev = Math.sqrt(squaredSum/count - avg*avg)
    sampleToEmit.setValues(endTimestamp, stdDev)
  }
}

class StdVarOverTimeFunction(var sum: Double = 0d,
                             var count: Int = 0,
                             var squaredSum: Double = 0d) extends RangeFunction {
  override def addedToWindow(row: TransientRow, window: Window): Unit = {
    sum += row.value
    squaredSum += row.value * row.value
    count += 1
  }

  override def removedFromWindow(row: TransientRow, window: Window): Unit = {
    sum -= row.value
    squaredSum -= row.value * row.value
    count -= 1
  }

  override def apply(startTimestamp: Long, endTimestamp: Long, window: Window,
                     sampleToEmit: TransientRow,
                     queryConfig: QueryConfig): Unit = {
    val avg = sum/count
    val stdVar = squaredSum/count - avg*avg
    sampleToEmit.setValues(endTimestamp, stdVar)
  }
}

abstract class VarOverTimeChunkedFunctionD(var sum: Double = Double.NaN,
                                           var count: Int = 0,
                                           var squaredSum: Double = Double.NaN,
                                           var lastSample: Double = Double.NaN) extends ChunkedDoubleRangeFunction {
  override final def reset(): Unit = { sum = Double.NaN; count = 0; squaredSum = Double.NaN }
  final def addTimeDoubleChunks(doubleVectAcc: MemoryReader,
                                doubleVect: BinaryVector.BinaryVectorPtr,
                                doubleReader: bv.DoubleVectorDataReader,
                                startRowNum: Int,
                                endRowNum: Int): Unit = {
    // Takes care of computing sum in all seq types : combination of NaN & not NaN & all numbers.
    val it = doubleReader.iterate(doubleVectAcc, doubleVect, startRowNum)
    var elemNo = startRowNum
    var chunkSum = Double.NaN
    var chunkSquaredSum = Double.NaN
    var chunkCount = 0
    while (elemNo <= endRowNum) {
      val nextValue = it.next
      if (!JLDouble.isNaN(nextValue)) {
        if (chunkSum.isNaN()) chunkSum = 0d
        if (chunkSquaredSum.isNaN()) chunkSquaredSum = 0d
        if (elemNo == endRowNum) lastSample = nextValue
        chunkSum += nextValue
        chunkSquaredSum += nextValue * nextValue
        chunkCount +=1
      }
      elemNo += 1
    }
    if(!JLDouble.isNaN(chunkSum) && JLDouble.isNaN(sum)) sum = 0d
    sum += chunkSum
    if(!JLDouble.isNaN(chunkSquaredSum) && JLDouble.isNaN(squaredSum)) squaredSum = 0d
    squaredSum += chunkSquaredSum
    count += chunkCount
  }
}

class StdDevOverTimeChunkedFunctionD extends VarOverTimeChunkedFunctionD() {
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    var stdDev = Double.NaN
    if (count > 0) {
      val avg = sum / count
      stdDev = Math.sqrt((squaredSum / count) - (avg * avg))
    }
    else if (sum.isNaN()) stdDev = sum
    else stdDev = 0d
    sampleToEmit.setValues(endTimestamp, stdDev)
  }
}

class StdVarOverTimeChunkedFunctionD extends VarOverTimeChunkedFunctionD() {
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    var stdVar = Double.NaN
    if (count > 0) {
      val avg = sum / count
      stdVar = (squaredSum / count) - (avg * avg)
    }
    else if (sum.isNaN()) stdVar = sum
    else stdVar = 0d
    sampleToEmit.setValues(endTimestamp, stdVar)
  }
}

abstract class VarOverTimeChunkedFunctionL(var sum: Double = 0d,
                                           var count: Int = 0,
                                           var squaredSum: Double = 0d) extends ChunkedLongRangeFunction {
  override final def reset(): Unit = { sum = 0d; count = 0; squaredSum = 0d }
  final def addTimeLongChunks(longVectAcc: MemoryReader,
                              longVect: BinaryVector.BinaryVectorPtr,
                              longReader: bv.LongVectorDataReader,
                              startRowNum: Int,
                              endRowNum: Int): Unit = {
    val it = longReader.iterate(longVectAcc, longVect, startRowNum)
    var _sum = 0d
    var _sqSum = 0d
    var elemNo = startRowNum
    while (elemNo <= endRowNum) {
      val nextValue = it.next.toDouble
      _sum += nextValue
      _sqSum += nextValue * nextValue
      elemNo += 1
    }
    count += (endRowNum - startRowNum + 1)
    sum += _sum
    squaredSum += _sqSum
  }
}

class StdDevOverTimeChunkedFunctionL extends VarOverTimeChunkedFunctionL() {
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    val avg = if (count > 0) sum/count else 0d
    val stdDev = Math.sqrt(squaredSum/count - avg*avg)
    sampleToEmit.setValues(endTimestamp, stdDev)
  }
}

class StdVarOverTimeChunkedFunctionL extends VarOverTimeChunkedFunctionL() {
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    val avg = if (count > 0) sum/count else 0d
    val stdVar = squaredSum/count - avg*avg
    sampleToEmit.setValues(endTimestamp, stdVar)
  }
}

abstract class ChangesChunkedFunction(var changes: Double = Double.NaN, var prev: Double = Double.NaN)
  extends ChunkedRangeFunction[TransientRow] {
  override final def reset(): Unit = { changes = Double.NaN; prev = Double.NaN }
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    sampleToEmit.setValues(endTimestamp, changes)
  }
}

class ChangesChunkedFunctionD() extends ChangesChunkedFunction() with
  ChunkedDoubleRangeFunction {
  final def addTimeDoubleChunks(doubleVectAcc: MemoryReader,
                                doubleVect: BinaryVector.BinaryVectorPtr,
                                doubleReader: bv.DoubleVectorDataReader,
                                startRowNum: Int,
                                endRowNum: Int): Unit = {
    if (changes.isNaN) {
      changes = 0d
    }

    val changesResult = doubleReader.changes(doubleVectAcc, doubleVect, startRowNum, endRowNum, prev)
    changes += changesResult._1
    prev = changesResult._2
  }
}

// scalastyle:off
class ChangesChunkedFunctionL extends ChangesChunkedFunction with
  ChunkedLongRangeFunction{
  final def addTimeLongChunks(longVectAcc: MemoryReader,
                              longVect: BinaryVector.BinaryVectorPtr,
                              longReader: bv.LongVectorDataReader,
                              startRowNum: Int,
                              endRowNum: Int): Unit = {
    if (changes.isNaN) {
      changes = 0d
    }
    val changesResult = longReader.changes(longVectAcc, longVect, startRowNum, endRowNum, prev.toLong)
    changes += changesResult._1
    prev = changesResult._2
  }
}

abstract class QuantileOverTimeChunkedFunction(funcParams: Seq[FuncArgs],
                                               var quantileResult: Double = Double.NaN,
                                               var values: Buffer[Double] = Buffer.empty[Double])
  extends ChunkedRangeFunction[TransientRow] {
  override final def reset(): Unit = { quantileResult = Double.NaN; values = Buffer.empty[Double] }
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    require(funcParams.head.isInstanceOf[StaticFuncArgs], "quantile parameter must be a number")
    val q = funcParams.head.asInstanceOf[StaticFuncArgs].scalar
    if (!quantileResult.equals(Double.NegativeInfinity) || !quantileResult.equals(Double.PositiveInfinity)) {
      val counter = values.length
      values.sort(spire.algebra.Order.fromOrdering[Double])
      val (weight, upperIndex, lowerIndex) = QuantileOverTimeFunction.calculateRank(q, counter)
      if (counter > 0) {
        quantileResult = values(lowerIndex)*(1-weight) + values(upperIndex)*weight
      }
    }
    sampleToEmit.setValues(endTimestamp, quantileResult)
  }

}

abstract class MedianAbsoluteDeviationOverTimeChunkedFunction(var medianAbsoluteDeviationResult: Double = Double.NaN,
                                                               var values: Buffer[Double] = Buffer.empty[Double])
  extends ChunkedRangeFunction[TransientRow] {
  override final def reset(): Unit = { medianAbsoluteDeviationResult = Double.NaN; values = Buffer.empty[Double] }
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    val size = values.length
    val (weight, upperIndex, lowerIndex) = QuantileOverTimeFunction.calculateRank(0.5, size)
    values.sort(spire.algebra.Order.fromOrdering[Double])
    var median: Double = Double.NaN
    if (size > 0) {
      median = values(lowerIndex) * (1 - weight) + values(upperIndex) * weight
      val diffFromMedians: Buffer[Double] = Buffer.ofSize(values.length)
      val iter = values.iterator()
      for (value <- values) {
        diffFromMedians.append(Math.abs(median - value))
      }
      diffFromMedians.sort(spire.algebra.Order.fromOrdering[Double])
      medianAbsoluteDeviationResult = diffFromMedians(lowerIndex) * (1 - weight) + diffFromMedians(upperIndex) * weight
    }
    sampleToEmit.setValues(endTimestamp, medianAbsoluteDeviationResult)
  }

}

class QuantileOverTimeChunkedFunctionD(funcParams: Seq[FuncArgs]) extends QuantileOverTimeChunkedFunction(funcParams)
  with ChunkedDoubleRangeFunction {
  require(funcParams.size == 1, "quantile_over_time function needs a single quantile argument")
  require(funcParams.head.isInstanceOf[StaticFuncArgs], "quantile parameter must be a number")
  final def addTimeDoubleChunks(doubleVectAcc: MemoryReader,
                                doubleVect: BinaryVector.BinaryVectorPtr,
                                doubleReader: bv.DoubleVectorDataReader,
                                startRowNum: Int,
                                endRowNum: Int): Unit = {
    //Only support StaticFuncArgs for now as we don't have time to get value from scalar vector
    val q = funcParams.head.asInstanceOf[StaticFuncArgs].scalar
    var counter = 0

    if (q < 0) quantileResult = Double.NegativeInfinity
    else if (q > 1) quantileResult = Double.PositiveInfinity
    else {
      var rowNum = startRowNum
      val it = doubleReader.iterate(doubleVectAcc, doubleVect, startRowNum)
      while (rowNum <= endRowNum) {
        var nextvalue = it.next
        // There are many possible values of NaN.  Use a function to ignore them reliably.
        if (!JLDouble.isNaN(nextvalue)) {
          values += nextvalue
        }
        rowNum += 1
      }
    }
  }
}

class MedianAbsoluteDeviationOverTimeChunkedFunctionD
  extends MedianAbsoluteDeviationOverTimeChunkedFunction
  with ChunkedDoubleRangeFunction {
  final def addTimeDoubleChunks(doubleVectAcc: MemoryReader,
                                doubleVect: BinaryVector.BinaryVectorPtr,
                                doubleReader: bv.DoubleVectorDataReader,
                                startRowNum: Int,
                                endRowNum: Int): Unit = {
    val it = doubleReader.iterate(doubleVectAcc, doubleVect, startRowNum)

    for (_ <- startRowNum to endRowNum) {
      val nextvalue = it.next
      // There are many possible values of NaN.  Use a function to ignore them reliably.
      if (!JLDouble.isNaN(nextvalue)) {
        values += nextvalue
      }
    }
  }
}

class QuantileOverTimeChunkedFunctionL(funcParams: Seq[FuncArgs])
  extends QuantileOverTimeChunkedFunction(funcParams) with ChunkedLongRangeFunction {
  require(funcParams.size == 1, "quantile_over_time function needs a single quantile argument")
  require(funcParams.head.isInstanceOf[StaticFuncArgs], "quantile parameter must be a number")
  final def addTimeLongChunks(longVectAcc: MemoryReader,
                              longVect: BinaryVector.BinaryVectorPtr,
                              longReader: bv.LongVectorDataReader,
                              startRowNum: Int,
                              endRowNum: Int): Unit = {
    val q = funcParams.head.asInstanceOf[StaticFuncArgs].scalar
    if (q < 0) quantileResult = Double.NegativeInfinity
    else if (q > 1) quantileResult = Double.PositiveInfinity
    else {
      var rowNum = startRowNum
      val it = longReader.iterate(longVectAcc, longVect, startRowNum)
      while (rowNum <= endRowNum) {
        var nextvalue = it.next
        values += nextvalue
        rowNum += 1
      }
    }
  }
}

class MedianAbsoluteDeviationOverTimeChunkedFunctionL
  extends MedianAbsoluteDeviationOverTimeChunkedFunction with ChunkedLongRangeFunction {
  final def addTimeLongChunks(longVectAcc: MemoryReader,
                              longVect: BinaryVector.BinaryVectorPtr,
                              longReader: bv.LongVectorDataReader,
                              startRowNum: Int,
                              endRowNum: Int): Unit = {
      val it = longReader.iterate(longVectAcc, longVect, startRowNum)
      for (_ <- startRowNum to endRowNum) {
        val nextvalue = it.next
        values += nextvalue
      }
  }
}

abstract class HoltWintersChunkedFunction(funcParams: Seq[FuncArgs],
                                          var b0: Double = Double.NaN,
                                          var s0: Double = Double.NaN,
                                          var nextvalue: Double = Double.NaN,
                                          var smoothedResult: Double = Double.NaN)
  extends ChunkedRangeFunction[TransientRow] {

  override final def reset(): Unit = { s0 = Double.NaN
                                       b0 = Double.NaN
                                       nextvalue = Double.NaN
                                       smoothedResult = Double.NaN }

  def parseParameters(funcParams: Seq[FuncArgs]): (Double, Double) = {
    require(funcParams.size == 2, "Holt winters needs 2 parameters")
    require(funcParams.head.isInstanceOf[StaticFuncArgs], "sf parameter must be a number")
    require(funcParams(1).isInstanceOf[StaticFuncArgs], "tf parameter must be a number")
    val sf = funcParams.head.asInstanceOf[StaticFuncArgs].scalar
    val tf = funcParams(1).asInstanceOf[StaticFuncArgs].scalar
    require(sf >= 0 & sf <= 1, "Sf should be in between 0 and 1")
    require(tf >= 0 & tf <= 1, "tf should be in between 0 and 1")
    (sf, tf)
  }

  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    sampleToEmit.setValues(endTimestamp, smoothedResult)
  }
}

/**
  * @param funcParams - Additional required function parameters
  * Refer https://en.wikipedia.org/wiki/Exponential_smoothing#Double_exponential_smoothing
  */
class HoltWintersChunkedFunctionD(funcParams: Seq[FuncArgs]) extends HoltWintersChunkedFunction(funcParams)
  with ChunkedDoubleRangeFunction {

  val (sf, tf) = parseParameters(funcParams)

  // Returns the first non-Nan value encountered
  def getNextValue(startRowNum: Int, endRowNum: Int, it: DoubleIterator): (Double, Int) = {
    var res = Double.NaN
    var currRowNum = startRowNum
    while (currRowNum <= endRowNum && JLDouble.isNaN(res)) {
      val nextvalue = it.next
      // There are many possible values of NaN.  Use a function to ignore them reliably.
      if (!JLDouble.isNaN(nextvalue)) {
        res = nextvalue
      }
      currRowNum += 1
    }
    (res, currRowNum)
  }

  final def addTimeDoubleChunks(doubleVectAcc: MemoryReader,
                                doubleVect: BinaryVector.BinaryVectorPtr,
                                doubleReader: bv.DoubleVectorDataReader,
                                startRowNum: Int,
                                endRowNum: Int): Unit = {
    val it = doubleReader.iterate(doubleVectAcc, doubleVect, startRowNum)
    var rowNum = startRowNum
    if (JLDouble.isNaN(s0) && JLDouble.isNaN(b0)) {
      // check if it is a new chunk
      val (_s0, firstrow) = getNextValue(startRowNum, endRowNum, it)
      val (_b0, currRow) = getNextValue(firstrow, endRowNum, it)
      nextvalue = _b0
      b0 = _b0 - _s0
      rowNum = currRow - 1
      s0 = _s0
    } else if (JLDouble.isNaN(b0)) {
      // check if the previous chunk had only one element
      val (_b0, currRow) = getNextValue(startRowNum, endRowNum, it)
      nextvalue = _b0
      b0 = _b0 - s0
      rowNum = currRow - 1
    }
    else {
      // continuation of a previous chunk
      it.next
    }
    if (!JLDouble.isNaN(b0)) {
      while (rowNum <= endRowNum) {
        // There are many possible values of NaN. Use a function to ignore them reliably.
        if (!JLDouble.isNaN(nextvalue)) {
          val _s0  = sf*nextvalue + (1-sf)*(s0 + b0)
          b0 = tf*(_s0 - s0) + (1-tf)*b0
          s0 = _s0
        }
        nextvalue = it.next
        rowNum += 1
      }
      smoothedResult = s0
    }
  }
}

class HoltWintersChunkedFunctionL(funcParams: Seq[FuncArgs]) extends HoltWintersChunkedFunction(funcParams)
  with ChunkedLongRangeFunction {

  val (sf, tf) = parseParameters(funcParams)

  final def addTimeLongChunks(longVectAcc: MemoryReader,
                              longVect: BinaryVector.BinaryVectorPtr,
                              longReader: bv.LongVectorDataReader,
                              startRowNum: Int,
                              endRowNum: Int): Unit = {
    val it = longReader.iterate(longVectAcc, longVect, startRowNum)
    var rowNum = startRowNum
    if (JLDouble.isNaN(b0)) {
      if (endRowNum - startRowNum >= 2) {
        s0 = it.next.toDouble
        b0 = it.next.toDouble
        nextvalue = b0
        b0 = b0 - s0
        rowNum = startRowNum + 1
      }
    } else {
      it.next
    }
    if (!JLDouble.isNaN(b0)) {
      while (rowNum <= endRowNum) {
        // There are many possible values of NaN.  Use a function to ignore them reliably.
        var nextvalue = it.next
        val smoothedResult  = sf*nextvalue + (1-sf)*(s0 + b0)
        b0 = tf*(smoothedResult - s0) + (1-tf)*b0
        s0 = smoothedResult
        nextvalue = it.next
        rowNum += 1
      }
    }
  }
}

/**
  * Predicts the value of time series t seconds from now based on range vector
  * Refer https://en.wikipedia.org/wiki/Simple_linear_regression
**/
abstract class PredictLinearChunkedFunction(funcParams: Seq[Any],
                                            var sumX: Double = Double.NaN,
                                            var sumY: Double = Double.NaN,
                                            var sumXY: Double = Double.NaN,
                                            var sumX2: Double = Double.NaN,
                                            var counter: Int = 0)
  extends ChunkedRangeFunction[TransientRow] {
  override final def reset(): Unit = { sumX = Double.NaN; sumY = Double.NaN;
    sumXY = Double.NaN; sumX2 = Double.NaN;
    counter = 0}
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    val duration = funcParams.head.asInstanceOf[StaticFuncArgs].scalar
    val covXY = sumXY - sumX*sumY/counter
    val varX = sumX2 - sumX*sumX/counter
    val slope = covXY / varX
    val intercept = sumY/counter - slope*sumX/counter // keeping it, needed for predict_linear function = slope*duration + intercept
    if (counter >= 2) {
      sampleToEmit.setValues(endTimestamp, slope*duration + intercept)
    } else {
      sampleToEmit.setValues(endTimestamp, Double.NaN)
    }
  }
}

class PredictLinearChunkedFunctionD(funcParams: Seq[Any]) extends PredictLinearChunkedFunction(funcParams)
  with ChunkedRangeFunction[TransientRow] {
  require(funcParams.size == 1, "predict_linear function needs a single time argument")
  require(funcParams.head.isInstanceOf[StaticFuncArgs], "duration parameter must be a number")
  final def addChunks(tsVectorAcc: MemoryReader, tsVector: BinaryVector.BinaryVectorPtr, tsReader: bv.LongVectorDataReader,
                      valueVectorAcc: MemoryReader, valueVector: BinaryVector.BinaryVectorPtr, valueReader: VectorDataReader,
                      startTime: Long, endTime: Long, info: ChunkSetInfoReader, queryConfig: QueryConfig): Unit = {
    var startRowNum = tsReader.binarySearch(tsVectorAcc, tsVector, startTime) & 0x7fffffff
    val endRowNum = Math.min(tsReader.ceilingIndex(tsVectorAcc, tsVector, endTime), info.numRows - 1)
    val itTimestamp = tsReader.asLongReader.iterate(tsVectorAcc, tsVector, startRowNum)
    val it = valueReader.asDoubleReader.iterate(valueVectorAcc, valueVector, startRowNum)
    while (startRowNum <= endRowNum) {
      val nextvalue = it.next
      val nexttime = itTimestamp.next
      // There are many possible values of NaN.  Use a function to ignore them reliably.
      if (!JLDouble.isNaN(nextvalue)) {
        val x = (nexttime-endTime)/1000.0
        if (sumY.isNaN) {
          sumY = nextvalue
          sumX = x
          sumXY = x * nextvalue
          sumX2 = x * x
        } else {
          sumY += nextvalue
          sumX += x
          sumXY += x * nextvalue
          sumX2 += x * x
        }
        counter += 1
      }
      startRowNum += 1
    }
  }
}


class PredictLinearChunkedFunctionL(funcParams: Seq[Any]) extends PredictLinearChunkedFunction(funcParams)
  with ChunkedRangeFunction[TransientRow] {
  require(funcParams.size == 1, "predict_linear function needs a single duration argument")
  require(funcParams.head.isInstanceOf[StaticFuncArgs], "duration parameter must be a number")
  final def addChunks(tsVectorAcc: MemoryReader, tsVector: BinaryVector.BinaryVectorPtr, tsReader: bv.LongVectorDataReader,
                      valueVectorAcc: MemoryReader, valueVector: BinaryVector.BinaryVectorPtr, valueReader: VectorDataReader,
                      startTime: Long, endTime: Long, info: ChunkSetInfoReader, queryConfig: QueryConfig): Unit = {
    var startRowNum = tsReader.binarySearch(tsVectorAcc, tsVector, startTime) & 0x7fffffff
    val endRowNum = Math.min(tsReader.ceilingIndex(tsVectorAcc, tsVector, endTime), info.numRows - 1)
    val itTimestamp = tsReader.asLongReader.iterate(tsVectorAcc, tsVector, startRowNum)
    val it = valueReader.asLongReader.iterate(valueVectorAcc, valueVector, startRowNum)
    while (startRowNum <= endRowNum) {
      val nextvalue = it.next
      val nexttime = itTimestamp.next
      val x = (nexttime-endTime)/1000.0
      if (sumY.isNaN) {
        sumY = nextvalue
        sumX = x
        sumXY = x * nextvalue
        sumX2 = x * x
      } else {
        sumY += nextvalue
        sumX += x
        sumXY += x * nextvalue
        sumX2 += x * x
      }
      counter += 1
      startRowNum += 1
    }
  }
}

/**
  * It represents the distance between the raw score and the population mean in units of the standard deviation.
  * Refer https://en.wikipedia.org/wiki/Standard_score#Calculation to understand how to calculate
  **/
class ZScoreChunkedFunctionD extends VarOverTimeChunkedFunctionD() {
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    var zscore = Double.NaN
    if (count > 0) {
      val avg = sum / count
      val stdDev = Math.sqrt(squaredSum / count - avg*avg)
      zscore = (lastSample - avg) / stdDev
    }
    else if (sum.isNaN()) zscore = sum
    else zscore = 0d
    sampleToEmit.setValues(endTimestamp, zscore)
  }
}