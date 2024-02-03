package io.univalence.advancedspark

import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, GreaterThanOrEqual}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, GlobalLimit, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy, datasources}
import org.apache.spark.sql.{DataFrame, SetOptim, SparkSession}
import zio.Task
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

  def sources(df: DataFrame): Seq[String] = {
    df.inputFiles
  }


  class MySubstitutionOptimisation(sparkSession: org.apache.spark.sql.SparkSession) extends Rule[LogicalPlan] {

    def addSubstitution(read: DataFrame, replaceWith: DataFrame): Unit = {
      MySubstitutionOptimisation.globalReplace.updateAndGet(_ :+ Substitution(read, replaceWith))
    }

    override def apply(plan: LogicalPlan): LogicalPlan = {
      val remplacements: Seq[Substitution] = MySubstitutionOptimisation.globalReplace.get()

      if (remplacements.nonEmpty) {


        val res = plan.transformDown(x => {
          val canon = x.canonicalized
          remplacements.find(_.read.queryExecution.analyzed.canonicalized == canon) match {
            case Some(Substitution(_, replaceWith)) =>

              val replacePlan = replaceWith.queryExecution.logical
              //zip could not work on case with different name orders
              //Project(x.output.zip(replacePlan.output).map({ case (a, b) => b.withExprId(a.exprId) }), replacePlan)
              //by keeping the plan logical, don't need to invalidate the metadata
              replacePlan
            case None => x
          }
        })
        if(res != plan) {

          println(plan.numberedTreeString)
          println(res.numberedTreeString)
        }

        res
      } else {
        plan
      }

    }
  }


  case class Substitution(read: DataFrame, replaceWith: DataFrame)

  object MySubstitutionOptimisation {
    //dirty fix
    val globalReplace: AtomicReference[Seq[Substitution]] = new AtomicReference(Nil)


  }

  def forceSubstitution(read: zio.spark.sql.DataFrame, replaceWith: zio.spark.sql.DataFrame): Task[Unit] = {
    zio.ZIO.attempt({
      MySubstitutionOptimisation.globalReplace.updateAndGet(subs => subs :+ Substitution(read.underlying, replaceWith.underlying))
    })
  }


}