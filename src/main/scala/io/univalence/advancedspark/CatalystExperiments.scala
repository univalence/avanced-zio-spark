package io.univalence.advancedspark

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.{DataFrame, SetOptim, SparkSession}
import zio.spark.sql.SIO


object CatalystExperiments {

  class MySubstitutionStrat(sparkSession: SparkSession, private var replaceWith: Seq[(DataFrame, DataFrame)]) extends SparkStrategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = ???

    def addSubstitution(read: DataFrame, replaceWith: DataFrame): Unit = {
      this.replaceWith = this.replaceWith :+ (read, replaceWith)
    }
  }


  def registerPotentialSubstitution(read: DataFrame, replaceWith: DataFrame): SIO[Unit] = {
    zio.spark.sql.SparkSession.attempt(ss => {
      ss.experimental.extraStrategies.collectFirst({
        case x: MySubstitutionStrat => x.addSubstitution(read, replaceWith)
      }).getOrElse({
        throw new Exception("MySubstitutionStrat not found")
      })
    })
  }


  class MySubstitutionOptimisation private (sparkSession: org.apache.spark.sql.SparkSession, private var replaceWith: Seq[(DataFrame, DataFrame)]) extends Rule[LogicalPlan] {

    def addSubstitution(read: DataFrame, replaceWith: DataFrame): Unit = {
      this.replaceWith = this.replaceWith :+ (read, replaceWith)
    }

    def this(sparkSession: org.apache.spark.sql.SparkSession) = this(sparkSession, Seq.empty)

    override def apply(plan: LogicalPlan): LogicalPlan = {


      plan
    }
  }

  def forceSubtitution(read: zio.spark.sql.DataFrame, replaceWith: zio.spark.sql.DataFrame): SIO[Unit] = {
    zio.spark.sql.SparkSession.attempt(ss => {

      SetOptim.getOptimFromSparkSession(ss) match {
        case Some(value) => value.addSubstitution(read.underlying, replaceWith.underlying)
        case None => throw new Exception("MySubstitutionOptimisation not found")
      }


    })
  }



}