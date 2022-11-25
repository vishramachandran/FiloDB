package filodb.query

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{PartitionRangeVectorKeySerializer, ZeroCopyUTF8StringSerializer}
import com.twitter.chill.ScalaKryoInstantiator
import com.typesafe.scalalogging.{Logger, StrictLogging}
import kamon.Kamon

import filodb.core.RecordSchema2Serializer
import filodb.core.binaryrecord2.RecordSchema
import filodb.core.query.{PartitionRangeVectorKey, SerializedRangeVector}
import filodb.memory.format.ZeroCopyUTF8String

/**
  * ExecPlan objects cannot have loggers as vals because they would then not be serializable.
  * All Query Engine constructs should use this logger and provide more details of the construct
  * in the log message.
  */
object Query extends StrictLogging {
  val qLogger: Logger = logger
  // TODO refine with dataset tag
  protected[query] val droppedSamples = Kamon.counter("query-dropped-samples").withoutTags

  val kryoThreadLocal = new ThreadLocal[Kryo]() {
    override def initialValue(): Kryo = {
      val instantiator = new ScalaKryoInstantiator
      val k = instantiator.newKryo()
      k.register(classOf[SerializedRangeVector])
      k.addDefaultSerializer(classOf[RecordSchema], classOf[RecordSchema2Serializer])
      k.register(classOf[PartitionRangeVectorKey], new PartitionRangeVectorKeySerializer)
      k.addDefaultSerializer(classOf[ZeroCopyUTF8String], classOf[ZeroCopyUTF8StringSerializer])
      k
    }
  }
}
