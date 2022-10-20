ThisBuild / version := "1.0"

ThisBuild / scalaVersion := "2.11.12"

val sparkVersion = "2.3.2"

lazy val root = (project in file("."))
  .settings(
    name := "users_items"
  )

libraryDependencies ++= Seq(
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.3",
  "org.elasticsearch" %% "elasticsearch-spark-20" % "7.6.2",
  "org.postgresql" % "postgresql" % "42.2.12",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "com.databricks" %% "spark-avro" % "4.0.0"
)
