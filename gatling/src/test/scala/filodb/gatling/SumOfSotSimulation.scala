package filodb.gatling

import scala.concurrent.duration.DurationInt

import ch.qos.logback.classic.{Level, LoggerContext}
import io.gatling.core.Predef._
import io.gatling.core.structure.ChainBuilder
import io.gatling.http.Predef._
import org.slf4j.LoggerFactory


object Configuration {
  val baseUrl = "http://localhost:8080"
  val numApps = 1
  val numUsers = 10
  val testDuration = 1.minutes
}

/**
 * TimeRangeQuerySimulation queries mosaic-queryservice with
 * random application name and kpi with configured user count asynchronously.
 *
 */
class SumOfSotSimulation extends Simulation {

  private val httpConfig = http.baseUrl("http://localhost:8080")

  val context: LoggerContext = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
  // Log all HTTP requests
  context.getLogger("io.gatling").setLevel(Level.valueOf("INFO"))

  private val jobNameFeeder = for {
    x <- 0 until Configuration.numApps
  } yield {
      Seq(Map("query" -> s"""sum(sum_over_time(heap_usage{_ns_="App-$x",_ws_="demo"}[5m]))""",
        "queryEndPointPrefix" -> "/promql/promperf"))
  }

  private val oneRepeat = jobNameFeeder.flatten
  private val jobFeeder = Iterator.continually(oneRepeat).flatten
  val start = 1602779088L // Before each run, change this to the right timestamp relevant to data stored in dev server
  val end = start + 3 * 60 * 60
  val timeRangeQuery: ChainBuilder = feed(jobFeeder)
    .exec(session => {
      session.set("startTime", start)
        .set("endTIme", end)
    })
    .exec(http("Query TimeRange Metric")
      .get("${queryEndPointPrefix}/api/v1/query_range")
      .queryParam("query", "${query}")
      .queryParam("start", "${startTime}")
      .queryParam("end", "${endTIme}")
      .queryParam("step", "60")
      .check(status is 200,
        jsonPath("$.status") is "success",
        jsonPath("$.data.result[*]").count.gte(1)))

  private val querySimulation = scenario("SumOfSumOverTime")

  setUp(
    querySimulation.exec(timeRangeQuery)
      .inject(constantUsersPerSec(Configuration.numUsers) during(Configuration.testDuration))
  ).protocols(httpConfig.shareConnections)

}
