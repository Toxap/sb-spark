ThisBuild / version := "1.0"

ThisBuild / scalaVersion := "2.11.12"

val sparkVersion = "2.3.2"

lazy val root = (project in file("."))
  .settings(
    name := "filter"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "com.databricks" %% "spark-avro" % "4.0.0"
)
