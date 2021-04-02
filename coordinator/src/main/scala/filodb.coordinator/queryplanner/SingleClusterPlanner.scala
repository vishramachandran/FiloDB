package filodb.coordinator.queryplanner

import scala.annotation.tailrec
import scala.concurrent.duration._

import akka.actor.ActorRef
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon

import filodb.coordinator.ShardMapper
import filodb.coordinator.client.QueryCommands.StaticSpreadProvider
import filodb.core.{DatasetRef, SpreadProvider}
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.Schemas
import filodb.core.query._
import filodb.core.store.{AllChunkScan, ChunkScanMethod, InMemoryChunkScan, TimeRangeChunkScan, WriteBufferChunkScan}
import filodb.prometheus.ast.Vectors.{PromMetricLabel, TypeLabel}
import filodb.prometheus.ast.WindowConstants
import filodb.query.{exec, _}
import filodb.query.exec.{LocalPartitionDistConcatExec, _}
import filodb.query.exec.InternalRangeFunction.Last

object SingleClusterPlanner {
  private val mdNoShardKeyFilterRequests = Kamon.counter("queryengine-metadata-no-shardkey-requests").withoutTags
}

/**
  * Responsible for query planning within single FiloDB cluster
  *
  * @param dsRef           dataset
  * @param schema          schema instance, used to extract partKey schema
  * @param spreadProvider  used to get spread
  * @param shardMapperFunc used to get shard locality
  * @param timeSplitEnabled split based on longer time range
  * @param minTimeRangeForSplitMs if time range is longer than this, plan will be split into multiple plans
  * @param splitSizeMs time range for each split, if plan needed to be split
  */
class SingleClusterPlanner(dsRef: DatasetRef,
                           schema: Schemas,
                           shardMapperFunc: => ShardMapper,
                           earliestRetainedTimestampFn: => Long,
                           queryConfig: QueryConfig,
                           spreadProvider: SpreadProvider = StaticSpreadProvider(),
                           timeSplitEnabled: Boolean = false,
                           minTimeRangeForSplitMs: => Long = 1.day.toMillis,
                           splitSizeMs: => Long = 1.day.toMillis)
                           extends QueryPlanner with StrictLogging with PlannerMaterializer {

  override val schemas = schema
  val shardColumns = dsOptions.shardKeyColumns.sorted

  import SingleClusterPlanner._

  private def dispatcherForShard(shard: Int): PlanDispatcher = {
    val targetActor = shardMapperFunc.coordForShard(shard)
    if (targetActor == ActorRef.noSender) {
      logger.debug(s"ShardMapper: $shardMapperFunc")
      throw new RuntimeException(s"Shard: $shard is not available") // TODO fix this
    }
    ActorPlanDispatcher(targetActor)
  }

  /**
   * Change start time of logical plan according to earliestRetainedTimestampFn
   */
  private def updateStartTime(logicalPlan: LogicalPlan): Option[LogicalPlan] = {
    val periodicSeriesPlan = LogicalPlanUtils.getPeriodicSeriesPlan(logicalPlan)
    if (periodicSeriesPlan.isEmpty) Some(logicalPlan)
    else {
      //For binary join LHS & RHS should have same times
      val boundParams = periodicSeriesPlan.get.head match {
        case p: PeriodicSeries => (p.startMs, p.stepMs, WindowConstants.staleDataLookbackMillis,
          p.offsetMs.getOrElse(0L), p.endMs)
        case w: PeriodicSeriesWithWindowing => (w.startMs, w.stepMs, w.window, w.offsetMs.getOrElse(0L), w.endMs)
        case _  => throw new UnsupportedOperationException(s"Invalid plan: ${periodicSeriesPlan.get.head}")
      }

      val newStartMs = boundToStartTimeToEarliestRetained(boundParams._1, boundParams._2, boundParams._3,
        boundParams._4)
      if (newStartMs <= boundParams._5) { // if there is an overlap between query and retention ranges
        if (newStartMs != boundParams._1)
          Some(LogicalPlanUtils.copyLogicalPlanWithUpdatedTimeRange(logicalPlan, TimeRange(newStartMs, boundParams._5)))
        else Some(logicalPlan)
      } else { // query is outside retention period, simply return empty result
        None
      }
    }
  }

  def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
    val plannerParams = qContext.plannerParams
    val updatedPlan = updateStartTime(logicalPlan)
    if (updatedPlan.isEmpty) EmptyResultExec(qContext, dsRef)
    else {
     val logicalPlan = updatedPlan.get
      val timeSplitConfig = if (plannerParams.timeSplitEnabled)
        (plannerParams.timeSplitEnabled, plannerParams.minTimeRangeForSplitMs, plannerParams.splitSizeMs)
      else (timeSplitEnabled, minTimeRangeForSplitMs, splitSizeMs)

      if (shardMapperFunc.numShards <= 0) throw new IllegalStateException("No shards available")
      val logicalPlans = if (updatedPlan.get.isInstanceOf[PeriodicSeriesPlan])
        LogicalPlanUtils.splitPlans(updatedPlan.get, qContext, timeSplitConfig._1,
          timeSplitConfig._2, timeSplitConfig._3)
      else
        Seq(logicalPlan)
      val materialized = logicalPlans match {
        case Seq(one) => materializeTimeSplitPlan(one, qContext)
        case many =>
          val materializedPlans = many.map(materializeTimeSplitPlan(_, qContext))
          val targetActor = pickDispatcher(materializedPlans)

          // create SplitLocalPartitionDistConcatExec that will execute child execplanss sequentially and stitches
          // results back with StitchRvsMapper transformer.
          val stitchPlan = SplitLocalPartitionDistConcatExec(qContext, targetActor, materializedPlans)
          stitchPlan
      }
      logger.debug(s"Materialized logical plan for dataset=$dsRef :" +
        s" $logicalPlan to \n${materialized.printTree()}")
      materialized
    }
  }

  private def materializeTimeSplitPlan(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
    val materialized = walkLogicalPlanTree(logicalPlan, qContext)
    match {
      case PlanResult(Seq(justOne), stitch) =>
        if (stitch) justOne.addRangeVectorTransformer(StitchRvsMapper())
        justOne
      case PlanResult(many, stitch) =>
        val targetActor = pickDispatcher(many)
        many.head match {
          case lve: LabelValuesExec => LabelValuesDistConcatExec(qContext, targetActor, many)
          case ske: PartKeysExec => PartKeysDistConcatExec(qContext, targetActor, many)
          case ep: ExecPlan =>
            val topPlan = LocalPartitionDistConcatExec(qContext, targetActor, many)
            if (stitch) topPlan.addRangeVectorTransformer(StitchRvsMapper())
            topPlan
        }
    }
    logger.debug(s"Materialized logical plan for dataset=$dsRef :" +
      s" $logicalPlan to \n${materialized.printTree()}")
    materialized
  }

  private def shardsFromFilters(filters: Seq[ColumnFilter],
                                qContext: QueryContext): Seq[Int] = {

    val spreadProvToUse = qContext.plannerParams.spreadOverride.getOrElse(spreadProvider)

    require(shardColumns.nonEmpty || qContext.plannerParams.shardOverrides.nonEmpty,
      s"Dataset $dsRef does not have shard columns defined, and shard overrides were not mentioned")

    qContext.plannerParams.shardOverrides.getOrElse {
      val shardVals = shardColumns.map { shardCol =>
        // So to compute the shard hash we need shardCol == value filter (exact equals) for each shardColumn
        filters.find(f => f.column == shardCol) match {
          case Some(ColumnFilter(_, Filter.Equals(filtVal: String))) =>
            shardCol -> RecordBuilder.trimShardColumn(dsOptions, shardCol, filtVal)
          case Some(ColumnFilter(_, filter)) =>
            throw new BadQueryException(s"Found filter for shard column $shardCol but " +
              s"$filter cannot be used for shard key routing")
          case _ =>
            throw new BadQueryException(s"Could not find filter for shard key column " +
              s"$shardCol, shard key hashing disabled")
        }
      }
      val metric = shardVals.find(_._1 == dsOptions.metricColumn)
        .map(_._2)
        .getOrElse(throw new BadQueryException(s"Could not find metric value"))
      val shardValues = shardVals.filterNot(_._1 == dsOptions.metricColumn).map(_._2)
      logger.debug(s"For shardColumns $shardColumns, extracted metric $metric and shard values $shardValues")
      val shardHash = RecordBuilder.shardKeyHash(shardValues, metric)
      shardMapperFunc.queryShards(shardHash, spreadProvToUse.spreadFunc(filters).last.spread)
    }
  }

  private def toChunkScanMethod(rangeSelector: RangeSelector): ChunkScanMethod = {
    rangeSelector match {
      case IntervalSelector(from, to) => TimeRangeChunkScan(from, to)
      case AllChunksSelector          => AllChunkScan
      case EncodedChunksSelector      => ???
      case WriteBufferSelector        => WriteBufferChunkScan
      case InMemoryChunksSelector     => InMemoryChunkScan
      case _                          => ???
    }
  }

  /**
    * Renames Prom AST __name__ metric name filters to one based on the actual metric column of the dataset,
    * if it is not the prometheus standard
    */
  private def renameMetricFilter(filters: Seq[ColumnFilter]): Seq[ColumnFilter] =
    if (dsOptions.metricColumn != PromMetricLabel) {
      filters map {
        case ColumnFilter(PromMetricLabel, filt) => ColumnFilter(dsOptions.metricColumn, filt)
        case other: ColumnFilter                 => other
      }
    } else {
      filters
    }

  /**
    * Walk logical plan tree depth-first and generate execution plans starting from the bottom
    *
    * @return ExecPlans that answer the logical plan provided
    */
  // scalastyle:off cyclomatic.complexity
  def walkLogicalPlanTree(logicalPlan: LogicalPlan,
                                  qContext: QueryContext): PlanResult = {
    logicalPlan match {
      case lp: RawSeries                   => materializeRawSeries(qContext, lp)
      case lp: RawChunkMeta                => materializeRawChunkMeta(qContext, lp)
      case lp: PeriodicSeries              => materializePeriodicSeries(qContext, lp)
      case lp: PeriodicSeriesWithWindowing => materializePeriodicSeriesWithWindowing(qContext, lp)
      case lp: ApplyInstantFunction        => materializeApplyInstantFunction(qContext, lp)
      case lp: ApplyInstantFunctionRaw     => materializeApplyInstantFunctionRaw(qContext, lp)
      case lp: Aggregate                   => materializeAggregate(qContext, lp)
      case lp: BinaryJoin                  => materializeBinaryJoin(qContext, lp)
      case lp: ScalarVectorBinaryOperation => materializeScalarVectorBinOp(qContext, lp)
      case lp: LabelValues                 => materializeLabelValues(qContext, lp)
      case lp: SeriesKeysByFilters         => materializeSeriesKeysByFilters(qContext, lp)
      case lp: ApplyMiscellaneousFunction  => materializeApplyMiscellaneousFunction(qContext, lp)
      case lp: ApplySortFunction           => materializeApplySortFunction(qContext, lp)
      case lp: ScalarVaryingDoublePlan     => materializeScalarPlan(qContext, lp)
      case lp: ScalarTimeBasedPlan         => materializeScalarTimeBased(qContext, lp)
      case lp: VectorPlan                  => materializeVectorPlan(qContext, lp)
      case lp: ScalarFixedDoublePlan       => materializeFixedScalar(qContext, lp)
      case lp: ApplyAbsentFunction         => materializeAbsentFunction(qContext, lp)
      case lp: ScalarBinaryOperation       => materializeScalarBinaryOperation(qContext, lp)
      case lp: SubQueryWithWindowing       => materializeSubqueryWithWindowing(qContext, lp)
    }
  }
  // scalastyle:on cyclomatic.complexity


  // scalastyle:off line.size.limit
  private def materializeSubqueryWithWindowing(qContext: QueryContext, lp: SubQueryWithWindowing): PlanResult = {

    /**
     * Example1:
     * min_over_time(<someQueryReturningInstantVector>[3m:1m])
     *
     * outerStart = S
     * outerEnd = E
     * outerStep = 90s
     *
     * Resulting Exec Plan:
     *
     * - ApplySubqueryRangeFunction MinOverTime from S to E, at lookback of 3m, innerStep of 60s, outerStep = 90s
     *     - Execute Range Query someQueryReturningInstantVector from start=S-3m until end=E at step=30 GCD(60,90)
     *
     *
     * Example2:
     * min_over_time(avg_over_time(<someNestedQueryReturningInstantVector>[6m:2m])[3m:1m])
     *
     * outerStart = S
     * outerEnd = E
     * outerStep = 90s
     *
     * - ApplySubqueryRangeFunction MinOverTime from S to E, at lookback of 3m, innerStep of 60s, outerStep = 90s
     *    - Execute Range Query sum(avg_over_time(<anotherNestedQueryReturningInstantVector>[6m:2m])) from start=S-3m until end=E at step=30s i.e. GCD(60s,90s)
     *         - ApplySubqueryRangeFunction AvgOverTime from S-3m to E, at lookback of 6m, innerStep of 120s, outerStep = 30s
     *                - Execute Range Query <anotherNestedQueryReturningInstantVector> from start=S-3m-6m until end=E at step=30s i.e. GCD(30s,120s)
     *
     */
    // scalastyle:on line.size.limit
    @tailrec def gcd(a: Long, b: Long): Long = if (b == 0) a else gcd(b, a % b)
    val innerQueryRange = TimeRange(lp.startMs - lp.subQueryWindowMs, lp.endMs)
    val innerQueryStepMs = gcd(lp.stepMs, lp.subQueryStepMs)
    val modifiedInnerPlan = LogicalPlanUtils.copyLogicalPlanWithUpdatedTimeRange(lp.inner,
      innerQueryRange, Some(innerQueryStepMs))
    val innerExecPlan = walkLogicalPlanTree(modifiedInnerPlan, qContext)
    val rangeFn = lp.function.map(f => InternalRangeFunction.lpToInternalFunc(f))
    innerExecPlan.plans.foreach { p =>
      p.addRangeVectorTransformer(ApplySubqueryRangeFunction(
        RangeParams(lp.startMs / 1000, lp.stepMs / 1000, lp.endMs / 1000),
        RangeParams(innerQueryRange.startMs / 1000, innerQueryStepMs / 1000, innerQueryRange.endMs / 1000),
        rangeFn, lp.subQueryWindowMs, lp.subQueryStepMs))
    }
    innerExecPlan
  }


  private def materializeBinaryJoin(qContext: QueryContext,
                                    lp: BinaryJoin): PlanResult = {
    val lhs = walkLogicalPlanTree(lp.lhs, qContext)
    val stitchedLhs = if (lhs.needsStitch) Seq(StitchRvsExec(qContext, pickDispatcher(lhs.plans), lhs.plans))
    else lhs.plans
    val rhs = walkLogicalPlanTree(lp.rhs, qContext)
    val stitchedRhs = if (rhs.needsStitch) Seq(StitchRvsExec(qContext, pickDispatcher(rhs.plans), rhs.plans))
    else rhs.plans

    val onKeysReal = ExtraOnByKeysUtil.getRealOnLabels(lp, queryConfig.addExtraOnByKeysTimeRanges)

    // TODO Currently we create separate exec plan node for stitching.
    // Ideally, we can go one step further and add capability to NonLeafNode plans to pre-process
    // and transform child results individually before composing child results together.
    // In theory, more efficient to use transformer than to have separate exec plan node to avoid IO.
    // In the interest of keeping it simple, deferring decorations to the ExecPlan. Add only if needed after measuring.

    val targetActor = pickDispatcher(stitchedLhs ++ stitchedRhs)
    val joined = if (lp.operator.isInstanceOf[SetOperator])
      Seq(exec.SetOperatorExec(qContext, targetActor, stitchedLhs, stitchedRhs, lp.operator,
        LogicalPlanUtils.renameLabels(onKeysReal, dsOptions.metricColumn),
        LogicalPlanUtils.renameLabels(lp.ignoring, dsOptions.metricColumn), dsOptions.metricColumn))
    else
      Seq(BinaryJoinExec(qContext, targetActor, stitchedLhs, stitchedRhs, lp.operator, lp.cardinality,
        LogicalPlanUtils.renameLabels(onKeysReal, dsOptions.metricColumn),
        LogicalPlanUtils.renameLabels(lp.ignoring, dsOptions.metricColumn),
        LogicalPlanUtils.renameLabels(lp.include, dsOptions.metricColumn), dsOptions.metricColumn))
    PlanResult(joined, false)
  }

  private def materializeAggregate(qContext: QueryContext,
                                   lp: Aggregate): PlanResult = {
    val toReduceLevel1 = walkLogicalPlanTree(lp.vectors, qContext)
    val reducer = addAggregator(lp, qContext, toReduceLevel1, queryConfig.addExtraOnByKeysTimeRanges)
    PlanResult(Seq(reducer), false) // since we have aggregated, no stitching
  }

  private def materializePeriodicSeriesWithWindowing(qContext: QueryContext,
                                                     lp: PeriodicSeriesWithWindowing): PlanResult = {
    val series = walkLogicalPlanTree(lp.series, qContext)
    val rawSource = lp.series.isRaw

    /* Last function is used to get the latest value in the window for absent_over_time
    If no data is present AbsentFunctionMapper will return range vector with value 1 */

    val execRangeFn = if (lp.function == RangeFunctionId.AbsentOverTime) Last
                      else InternalRangeFunction.lpToInternalFunc(lp.function)

    val paramsExec = materializeFunctionArgs(lp.functionArgs, qContext)
    val window = if (execRangeFn == InternalRangeFunction.Timestamp) None else Some(lp.window)
    series.plans.foreach(_.addRangeVectorTransformer(PeriodicSamplesMapper(lp.startMs, lp.stepMs,
      lp.endMs, window, Some(execRangeFn), qContext, lp.stepMultipleNotationUsed,
      paramsExec, lp.offsetMs, rawSource)))
    if (lp.function == RangeFunctionId.AbsentOverTime) {
      val aggregate = Aggregate(AggregationOperator.Sum, lp, Nil, Seq("job"))
      // Add sum to aggregate all child responses
      // If all children have NaN value, sum will yield NaN and AbsentFunctionMapper will yield 1
      val aggregatePlanResult = PlanResult(Seq(addAggregator(aggregate, qContext.copy(plannerParams =
        qContext.plannerParams.copy(skipAggregatePresent = true)), series, Seq.empty)))
      addAbsentFunctionMapper(aggregatePlanResult, lp.columnFilters,
        RangeParams(lp.startMs / 1000, lp.stepMs / 1000, lp.endMs / 1000), qContext)
    } else series
  }

  private def materializePeriodicSeries(qContext: QueryContext,
                                        lp: PeriodicSeries): PlanResult = {
    val rawSeries = walkLogicalPlanTree(lp.rawSeries, qContext)
      rawSeries.plans.foreach(_.addRangeVectorTransformer(PeriodicSamplesMapper(lp.startMs, lp.stepMs, lp.endMs,
        None, None, qContext, false, Nil, lp.offsetMs)))
   rawSeries
  }

  /**
    * Calculates the earliest startTime possible given the query's start/window/step/offset.
    * This is used to bound the startTime of queries so we dont create possibility of aggregating
    * partially expired data and return incomplete results.
    *
    * @return new startTime to be used for query in ms. If original startTime is within retention
    *         period, returns it as is.
    */
  private def boundToStartTimeToEarliestRetained(startMs: Long, stepMs: Long,
                                                 windowMs: Long, offsetMs: Long): Long = {
    // In case query is earlier than earliestRetainedTimestamp then we need to drop the first few instants
    // to prevent inaccurate results being served. Inaccuracy creeps in because data can be in memory for which
    // equivalent data may not be in cassandra. Aggregations cannot be guaranteed to be complete.
    // Also if startMs < 500000000000 (before 1985), it is usually a unit test case with synthetic data, so don't fiddle
    // around with those queries
    val earliestRetainedTimestamp = earliestRetainedTimestampFn
    if (startMs - windowMs - offsetMs < earliestRetainedTimestamp && startMs > 500000000000L) {
      // We calculate below number of steps/instants to drop. We drop instant if data required for that instant
      // doesnt fully fall into the retention period. Data required for that instant involves
      // going backwards from that instant up to windowMs + offsetMs milli-seconds.
      val numStepsBeforeRetention = (earliestRetainedTimestamp - startMs + windowMs + offsetMs) / stepMs
      val lastInstantBeforeRetention = startMs + numStepsBeforeRetention * stepMs
      lastInstantBeforeRetention + stepMs
    } else {
      startMs
    }
  }

  /**
    * If there is a _type_ filter, remove it and populate the schema name string.  This is because while
    * the _type_ filter is a real filter on time series, we don't index it using Lucene at the moment.
    */
  private def extractSchemaFilter(filters: Seq[ColumnFilter]): (Seq[ColumnFilter], Option[String]) = {
    var schemaOpt: Option[String] = None
    val newFilters = filters.filterNot { case ColumnFilter(label, filt) =>
      val isTypeFilt = label == TypeLabel
      if (isTypeFilt) filt match {
        case Filter.Equals(schema) => schemaOpt = Some(schema.asInstanceOf[String])
        case x: Any                 => throw new IllegalArgumentException(s"Illegal filter $x on _type_")
      }
      isTypeFilt
    }
    (newFilters, schemaOpt)
  }

  private def materializeRawSeries(qContext: QueryContext,
                                   lp: RawSeries): PlanResult = {
    val spreadProvToUse = qContext.plannerParams.spreadOverride.getOrElse(spreadProvider)
    val offsetMillis: Long = lp.offsetMs.getOrElse(0)
    val colName = lp.columns.headOption
    val (renamedFilters, schemaOpt) = extractSchemaFilter(renameMetricFilter(lp.filters))
    val spreadChanges = spreadProvToUse.spreadFunc(renamedFilters)
    val rangeSelectorWithOffset = lp.rangeSelector match {
      case IntervalSelector(fromMs, toMs) => IntervalSelector(fromMs - offsetMillis - lp.lookbackMs.getOrElse(
                                             queryConfig.staleSampleAfterMs), toMs - offsetMillis)
      case _                              => lp.rangeSelector
    }
    val needsStitch = rangeSelectorWithOffset match {
      case IntervalSelector(from, to) => spreadChanges.exists(c => c.time >= from && c.time <= to)
      case _                          => false
    }
    val execPlans = shardsFromFilters(renamedFilters, qContext).map { shard =>
      val dispatcher = dispatcherForShard(shard)
      MultiSchemaPartitionsExec(qContext, dispatcher, dsRef, shard, renamedFilters,
        toChunkScanMethod(rangeSelectorWithOffset), schemaOpt, colName)
    }
    PlanResult(execPlans, needsStitch)
  }

  private def materializeLabelValues(qContext: QueryContext,
                                     lp: LabelValues): PlanResult = {
    // If the label is PromMetricLabel and is different than dataset's metric name,
    // replace it with dataset's metric name. (needed for prometheus plugins)
    val metricLabelIndex = lp.labelNames.indexOf(PromMetricLabel)
    val labelNames = if (metricLabelIndex > -1 && dsOptions.metricColumn != PromMetricLabel)
      lp.labelNames.updated(metricLabelIndex, dsOptions.metricColumn) else lp.labelNames

    val renamedFilters = renameMetricFilter(lp.filters)
    val shardsToHit = if (shardColumns.toSet.subsetOf(renamedFilters.map(_.column).toSet)) {
      shardsFromFilters(renamedFilters, qContext)
    } else {
      mdNoShardKeyFilterRequests.increment()
      shardMapperFunc.assignedShards
    }
    val metaExec = shardsToHit.map { shard =>
      val dispatcher = dispatcherForShard(shard)
      exec.LabelValuesExec(qContext, dispatcher, dsRef, shard, renamedFilters, labelNames, lp.startMs, lp.endMs)
    }
    PlanResult(metaExec, false)
  }

  private def materializeSeriesKeysByFilters(qContext: QueryContext,
                                             lp: SeriesKeysByFilters): PlanResult = {
    // NOTE: _type_ filter support currently isn't there in series keys queries
    val (renamedFilters, schemaOpt) = extractSchemaFilter(renameMetricFilter(lp.filters))
    val filterCols = lp.filters.map(_.column).toSet
    val shardsToHit = if (shardColumns.toSet.subsetOf(filterCols)) {
      shardsFromFilters(lp.filters, qContext)
    } else {
      mdNoShardKeyFilterRequests.increment()
      shardMapperFunc.assignedShards
    }
    val metaExec = shardsToHit.map { shard =>
      val dispatcher = dispatcherForShard(shard)
      PartKeysExec(qContext, dispatcher, dsRef, shard, renamedFilters,
                   lp.fetchFirstLastSampleTimes, lp.startMs, lp.endMs)
    }
    PlanResult(metaExec, false)
  }

  private def materializeRawChunkMeta(qContext: QueryContext,
                                      lp: RawChunkMeta): PlanResult = {
    // Translate column name to ID and validate here
    val colName = if (lp.column.isEmpty) None else Some(lp.column)
    val (renamedFilters, schemaOpt) = extractSchemaFilter(renameMetricFilter(lp.filters))
    val metaExec = shardsFromFilters(renamedFilters, qContext).map { shard =>
      val dispatcher = dispatcherForShard(shard)
      SelectChunkInfosExec(qContext, dispatcher, dsRef, shard, renamedFilters, toChunkScanMethod(lp.rangeSelector),
        schemaOpt, colName)
    }
    PlanResult(metaExec, false)
  }

  private def materializeScalarTimeBased(qContext: QueryContext,
                                         lp: ScalarTimeBasedPlan): PlanResult = {
    val scalarTimeBasedExec = TimeScalarGeneratorExec(qContext, dsRef, lp.rangeParams, lp.function)
    PlanResult(Seq(scalarTimeBasedExec), false)
  }

  private def materializeFixedScalar(qContext: QueryContext,
                                     lp: ScalarFixedDoublePlan): PlanResult = {
    val scalarFixedDoubleExec = ScalarFixedDoubleExec(qContext, dsRef, lp.timeStepParams, lp.scalar)
    PlanResult(Seq(scalarFixedDoubleExec), false)
  }

  private def materializeScalarBinaryOperation(qContext: QueryContext,
                                               lp: ScalarBinaryOperation): PlanResult = {
    val lhs = if (lp.lhs.isRight) {
      // Materialize as lhs is a logical plan
      val lhsExec = walkLogicalPlanTree(lp.lhs.right.get, qContext)
      Right(lhsExec.plans.map(_.asInstanceOf[ScalarBinaryOperationExec]).head)
    } else Left(lp.lhs.left.get)

    val rhs = if (lp.rhs.isRight) {
      val rhsExec = walkLogicalPlanTree(lp.rhs.right.get, qContext)
      Right(rhsExec.plans.map(_.asInstanceOf[ScalarBinaryOperationExec]).head)
    } else Left(lp.rhs.left.get)

    val scalarBinaryExec = ScalarBinaryOperationExec(qContext, dsRef, lp.rangeParams, lhs, rhs, lp.operator)
    PlanResult(Seq(scalarBinaryExec), false)
  }

}
