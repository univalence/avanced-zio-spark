package example

import zio._
import zio.spark.parameter._
import zio.spark.sql._
import zio.spark.sql.implicits._
final case class Person(name: String, age: Int)

object SimpleApp extends ZIOAppDefault {
  //force throwing exeptions from analysis, like missing columns, invalid sql, ...
  import zio.spark.sql.TryAnalysis.syntax.throwAnalysisException


  def read: SIO[DataFrame] =
    SparkSession.read.schema[Person]
      .withHeader
      .withDelimiter(";")
      .csv("./src/main/resources/data.csv")

  def job(inputDs: DataFrame) = inputDs.as[Person].headOption


  val app: ZIO[SparkSession, Throwable, Unit] =
    for {
      input <- read
      maybePeople <- job(input)
      _ <- maybePeople match {
          case None => Console.printLine("There is nobody :(.")
          case Some(p) => Console.printLine(s"The first person's name is ${p.name}.")
        }
    } yield ()

  private val session = SparkSession.builder.master(localAllNodes).appName("app").asLayer

  override def run: ZIO[ZIOAppArgs, Any, Any] = app.provide(session)
}