package io.univalence.advancedspark

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.{DataFrame, SetOptim, SparkSession}
import zio.spark.sql.SIO

import java.util.concurrent.atomic.AtomicReference


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


  class MySubstitutionOptimisation private (sparkSession: org.apache.spark.sql.SparkSession) extends Rule[LogicalPlan] {

    def addSubstitution(read: DataFrame, replaceWith: DataFrame): Unit = {
      MySubstitutionOptimisation.globalReplace.updateAndGet(_ :+ (read, replaceWith))
    }

    override def apply(plan: LogicalPlan): LogicalPlan = {
      val remplacements = MySubstitutionOptimisation.globalReplace.get()
    /*
      replaceWith.foreach({
        case (read, replaceWith) => {
          plan.transformUp({
            case x if x == read.queryExecution.plan => replaceWith.queryExecution.optimizedPlan
          })
        }
      }*/
      plan
    }
  }

  object MySubstitutionOptimisation {
    //dirty fix
    val globalReplace: AtomicReference[Seq[(DataFrame, DataFrame)]] = new AtomicReference(Nil)


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