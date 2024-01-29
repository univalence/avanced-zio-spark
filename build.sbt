ThisBuild / scalaVersion := "2.13.12"

ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.example"
ThisBuild / organizationName := "example"

val dependencies = new {
  lazy val zio = "2.1-RC1"
  lazy val zioSpark = "0.12.0"
  lazy val spark = "3.5.0"
}

lazy val root = (project in file("."))
  .settings(
    name := "advanced zio spark",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio-direct" % "1.0.0-RC7",
      "dev.zio" %% "zio" % dependencies.zio,
      "dev.zio" %% "zio-test" % dependencies.zio % Test,
      "dev.zio" %% "zio-test-sbt" % dependencies.zio % Test,


      "io.univalence" %% "zio-spark" % dependencies.zioSpark,

      "org.apache.spark" %% "spark-core" % dependencies.spark,
      "org.apache.spark" %% "spark-sql" % dependencies.spark
    )
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
