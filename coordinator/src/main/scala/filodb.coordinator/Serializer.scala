package filodb.coordinator

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.boon.primitive.{ByteBuf, InputByteArray}
import org.velvia.filo.RowReader

import filodb.coordinator.IngestionCommands.IngestRows
import filodb.core._
import filodb.core.binaryrecord.{BinaryRecord, RecordSchema}
import filodb.core.memstore.IngestRecord

/**
 * Utilities for special serializers for row data (for efficient record routing).
 *
 * We also maintain some global state for mapping a schema hashcode to the actual schema object.  This must
 * be updated externally.  The reason this is decided is because IngestRows is sent between a RowSource
 * client and node ingestor actors, both of whom have exchanged and know the schema already, thus there is
 * no need to pay the price of transmitting the schema itself with every IngestRows object.
 */
object Serializer extends StrictLogging {
  implicit class RichIngestRows(data: IngestRows) {
    /**
     * Converts a IngestRows into bytes, assuming all RowReaders are in fact BinaryRecords
     */
    def toBytes(): Array[Byte] =
      serializeIngestRows(data)
  }

  def serializeIngestRows(data: IngestRows): Array[Byte] = {
    val buf = ByteBuf.create(1000)
    val (partSchemaHash, dataSchemaHash) = data.rows.headOption match {
      case Some(IngestRecord(p: BinaryRecord, d: BinaryRecord, _)) =>
        (p.schema.hashCode, d.schema.hashCode)
      case other: Any            => (-1, -1)
    }
    buf.writeInt(partSchemaHash)
    buf.writeInt(dataSchemaHash)
    buf.writeMediumString(data.dataset.dataset)
    buf.writeMediumString(data.dataset.database.getOrElse(""))
    buf.writeInt(data.version)
    buf.writeInt(data.shard)
    buf.writeInt(data.rows.length)
    for { record <- data.rows } {
      record match {
        case IngestRecord(p: BinaryRecord, d: BinaryRecord, offset) =>
          buf.writeMediumByteArray(p.bytes)
          buf.writeMediumByteArray(d.bytes)
          buf.writeLong(offset)
      }
    }
    buf.toBytes
  }

  def fromBinaryIngestRows(bytes: Array[Byte]): IngestRows = {
    val scanner = new InputByteArray(bytes)
    val partSchemaHash = scanner.readInt
    val dataSchemaHash = scanner.readInt
    val dataset = scanner.readMediumString
    val db = Option(scanner.readMediumString).filter(_.length > 0)
    val version = scanner.readInt
    val shard = scanner.readInt
    val numRows = scanner.readInt
    val rows = new collection.mutable.ArrayBuffer[IngestRecord]()
    if (numRows > 0) {
      val partSchema = partSchemaMap.get(partSchemaHash)
      val dataSchema = dataSchemaMap.get(dataSchemaHash)
      if (Option(partSchema).isEmpty) { logger.error(s"Schema with hash $partSchemaHash not found!") }
      for { i <- 0 until numRows } {
        rows += IngestRecord(BinaryRecord(partSchema, scanner.readMediumByteArray),
                             BinaryRecord(dataSchema, scanner.readMediumByteArray),
                             scanner.readLong)
      }
    }
    IngestionCommands.IngestRows(DatasetRef(dataset, db), version, shard, rows)
  }

  private val partSchemaMap = new java.util.concurrent.ConcurrentHashMap[Int, RecordSchema]
  private val dataSchemaMap = new java.util.concurrent.ConcurrentHashMap[Int, RecordSchema]

  def putPartitionSchema(schema: RecordSchema): Unit = {
    logger.debug(s"Saving partition schema with fields ${schema.fields.toList} and hash ${schema.hashCode}...")
    partSchemaMap.put(schema.hashCode, schema)
  }

  def putDataSchema(schema: RecordSchema): Unit = {
    logger.debug(s"Saving data schema with fields ${schema.fields.toList} and hash ${schema.hashCode}...")
    dataSchemaMap.put(schema.hashCode, schema)
  }
}

/**
 * A special serializer to use the fast binary IngestRows format instead of much slower Java serialization
 * To solve the problem of the serializer not knowing the schema beforehand, it is the responsibility of
 * the DatasetCoordinatorActor to update the schema when it changes.  The hash of the RecordSchema is
 * sent and received.
 */
class IngestRowsSerializer extends akka.serialization.Serializer {
  import Serializer._

  // This is whether "fromBinary" requires a "clazz" or not
  def includeManifest: Boolean = false

  def identifier: Int = 1001

  def toBinary(obj: AnyRef): Array[Byte] = obj match {
    case i: IngestRows => i.toBytes()
  }

  def fromBinary(bytes: Array[Byte],
                 clazz: Option[Class[_]]): AnyRef = {
    fromBinaryIngestRows(bytes)
  }
}