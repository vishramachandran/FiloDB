package filodb.gatling

import io.gatling.app.Gatling
import io.gatling.core.config.GatlingPropertiesBuilder

object GatlingDriver extends App {

  val simClass = classOf[BinaryJoinSimulation].getName

  val props = new GatlingPropertiesBuilder
//  props.sourcesDirectory("./src/main/scala")
  props.binariesDirectory("./target/scala-2.12/classes")
  props.simulationClass(simClass)
  props.runDescription("Sum Of Sum Over Time")
  props.resultsDirectory("results-" + System.currentTimeMillis())

//  // This checks the values set in gatling_kickoff.rb
//  if (sys.env("PUPPET_GATLING_REPORTS_ONLY") == "true") {
//    props.reportsOnly(sys.env("PUPPET_GATLING_REPORTS_TARGET"))
//  }

  Gatling.fromMap(props.build)

}
