package filodb.query

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.twitter.chill.ScalaKryoInstantiator
import com.typesafe.scalalogging.{Logger, StrictLogging}
import kamon.Kamon

import filodb.core.query.{PartitionRangeVectorKey, SerializedRangeVector}

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
      k
    }
  }


}
