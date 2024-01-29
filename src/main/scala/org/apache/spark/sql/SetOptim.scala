package org.apache.spark.sql

import io.univalence.advancedspark.CatalystExperiments.MySubstitutionOptimisation

import scala.util.Try

object SetOptim {


  def getOptimFromSparkSession(sparkSession: SparkSession):Option[MySubstitutionOptimisation] = {
    sparkSession.extensions.buildOptimizerRules(sparkSession).collectFirst({
      case x: MySubstitutionOptimisation => x
    })
  }

}
