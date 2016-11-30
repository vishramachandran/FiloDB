package filodb.core.binaryrecord

import org.boon.primitive.ByteBuf
import scala.language.existentials

import filodb.core.metadata.Column
import filodb.core.metadata.Column.ColumnType

final case class Field(num: Int, colType: ColumnType, fixedDataOffset: Int, fieldType: FieldType[_]) {
  final def get[T](record: BinaryRecord): T = fieldType.asInstanceOf[FieldType[T]].extract(record, this)
  final def getAny(record: BinaryRecord): Any = fieldType.extract(record, this)
  final def writeSortable(record: BinaryRecord, buf: ByteBuf): Unit =
    fieldType.writeSortable(record, this, buf)
}

/**
 * Stores offsets and other information for a BinaryRecord for a given schema (seq of column types)
 */
class RecordSchema(columnTypes: Seq[ColumnType]) {
  // Computes offsets for every field, where they would go etc
  val numFields = columnTypes.length

  // Number of 32-bit words at beginning for null check
  val nullBitWords = (numFields + 31) / 32
  val fixedDataStartOffset = nullBitWords * 4

  // val fields - fixed data field section
  var curOffset = fixedDataStartOffset
  val fields = columnTypes.zipWithIndex.map { case (colType, no) =>
    val field = Field(no, colType, curOffset, FieldType.columnToField(colType))
    curOffset += field.fieldType.numFixedBytes
    field
  }.toArray

  val variableDataStartOffset = curOffset
}

object RecordSchema {
  def apply(columns: Seq[Column]): RecordSchema = new RecordSchema(columns.map(_.columnType))
}