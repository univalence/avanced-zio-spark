package io.univalence.advancedspark

import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, GreaterThanOrEqual}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, GlobalLimit, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy, datasources}
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

  def source(df: DataFrame): List[String] = {
    (df.queryExecution.logical +: df.queryExecution.logical.children)
      .filter(plan => plan.isInstanceOf[LogicalRelation])
      .flatMap(
        {case LogicalRelation(HadoopFsRelation(location, _, _, _, _, _), _ ,_, _) => location.inputFiles}
      )
      .toList
  }


  class MySubstitutionOptimisation (sparkSession: org.apache.spark.sql.SparkSession) extends Rule[LogicalPlan] {

    def addSubstitution(read: DataFrame, replaceWith: DataFrame): Unit = {
      MySubstitutionOptimisation.globalReplace.updateAndGet(_ :+ (read, replaceWith))
    }

    override def apply(plan: LogicalPlan): LogicalPlan = {
      val remplacements: Seq[(DataFrame, DataFrame)] = MySubstitutionOptimisation.globalReplace.get()
    /*
      replaceWith.foreach({
        case (read, replaceWith) => {
          plan.transformUp({
            case x if x == read.queryExecution.plan => replaceWith.queryExecution.optimizedPlan
          })
        }
      }*/

      if(remplacements.nonEmpty)
        {
          val original_plan = remplacements(0)._1.queryExecution.commandExecuted
          val new_plan = remplacements(0)._2.queryExecution.logical
          plan transformUp {
            //case Filter(Cast(_ ,_, _, _),_) => //new_plan
            case x: Filter if x.condition == original_plan.asInstanceOf[Filter].condition =>
              new_plan
            case _ => plan
          }
        }
      else plan

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