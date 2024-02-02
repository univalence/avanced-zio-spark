package io.univalence.advancedspark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SaveMode}
import zio.direct._
import zio.{Scope, ZIO, ZLayer}
import zio.spark.sql.{DataFrame, Dataset, SparkSession}
import zio.test._

import java.sql.Savepoint


object CatalystExperimentsSpec extends ZIOSpecDefault {


  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("CatalystExperimentsSpec") {

      import zio.spark.sql.TryAnalysis.syntax.throwAnalysisException

      val prg: ZIO[SparkSession, Throwable, TestResult] = defer {

        val data: DataFrame = SparkSession.read
          .option("header", "true")
          .option("delimiter",";")
          .csv("src/main/resources/data.csv").run

        data.createOrReplaceTempView("people").run

        val adults: DataFrame = data.where("age >= 18").getOrThrow

        val analysis1 = adults.get(_.queryExecution).sparkPlan

        val target = "target/tmp/data.parquet"

        //adults.write.mode(SaveMode.Overwrite).parquet(target).run
        //TODO créer un parquet en enlevant un adulte, puis ne plus l'écraser, et rajouter l'adulte, en tout cas c'est que j'avais fait

        val adultsFromDisk: DataFrame = SparkSession.read.parquet(target).run

        adults
          .withColumn("test", input_file_name()).show(false).run

        CatalystExperiments.forceSubtitution(adults, adultsFromDisk).run

        val analysis2 = data.where("age >= 18").get(_.queryExecution).optimizedPlan

        val analysis3 = data.where("age >= 16").get(_.queryExecution).optimizedPlan

        val test = data.where("age >= 18")
          //.withColumn("test", input_file_name())
          //.withColumn("eee", lit("test"))
        test.show(false).run

        data.where("age >= 16")
          //.withColumn("test", input_file_name())
          //.withColumn("eee", lit("test"))
          .show(false).run


        /*
        adultsFromDisk
          .withColumn("test", input_file_name()).show(false).run

         */

        CatalystExperiments.source(adults.underlying).map(println)
        CatalystExperiments.source(adultsFromDisk.underlying).map(println)


        assertTrue(true)

      }

      test("CatalystExperimentsSpec")(prg).provideLayer(
        ZLayer(
          ZIO.attempt(SparkSession(
            org.apache.spark.sql.SparkSession.builder()
              .master("local[*]")
              .appName("test")
              .withExtensions(_.injectOptimizerRule(new CatalystExperiments.MySubstitutionOptimisation(_)))
              .getOrCreate()
          ))
        )
      )


    }
}
