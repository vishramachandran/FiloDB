package filodb.query.exec

import monix.reactive.Observable

import filodb.core.query._
import filodb.query.exec.rangefn.RangeFunction

final case class ApplySubqueryRangeFunction(outputRange: RangeParams,
                                            inputRange: RangeParams,
                                            subqueryRangeFunctionId: Option[InternalRangeFunction],
                                            subqueryLookbackMs: Long,
                                            subqueryStepMs: Long
                                           ) extends RangeVectorTransformer {
  override def funcParams: Seq[FuncArgs] = Nil

  override protected[exec] def args: String = s"inputRange=$inputRange,outputRange=$outputRange," +
    s"subqueryRangeFunctionId=$subqueryRangeFunctionId," +
    s"subqueryLookbackMs=$subqueryLookbackMs,subqueryStepMs=$subqueryStepMs"

  def apply(source: Observable[RangeVector],
            querySession: QuerySession,
            limit: Int,
            sourceSchema: ResultSchema, paramsResponse: Seq[Observable[ScalarRangeVector]]): Observable[RangeVector] = {

    val inputRangeStepMs = inputRange.stepSecs * 1000
    require(subqueryStepMs % inputRangeStepMs == 0, s"For some reason subqueryStepMs=$subqueryStepMs " +
      s"is not divisible by inputRangeStepMs=$inputRangeStepMs. Planner should have used GCD. Check why this " +
      s"didn't happen.")

    val skip = (subqueryStepMs / inputRangeStepMs).toInt // Need to skip these many samples for window operations
    val valColType = RangeVectorTransformer.valueColumnType(sourceSchema)
    val rangeFuncGen = RangeFunction.generatorFor(sourceSchema,
      subqueryRangeFunctionId, valColType, querySession.queryConfig, funcParams, false, Some(skip))

    source.map { rv =>
      IteratorBackedRangeVector(rv.key,
        new SlidingWindowIterator(rv.rows, outputRange.startSecs * 1000,
          outputRange.stepSecs * 1000, outputRange.endSecs * 1000, subqueryLookbackMs,
          rangeFuncGen().asSliding, querySession.queryConfig))
    }
  }

}
