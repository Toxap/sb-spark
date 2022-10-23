import org.apache.spark.sql.functions.{callUDF, col, concat, count, desc, explode, from_unixtime, lit, lower, regexp_replace, when}
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object features extends App {
  val spark: SparkSession = SparkSession.builder()
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()

  val matrix = spark.read.format("parquet").load("/user/anton.khapinskiy/users-items/20200429/")

  val logs: DataFrame = spark.read.json("hdfs:///labs/laba03/weblogs.json").withColumn("visit", explode(col("visits"))).where(col("visit.url").isNotNull).withColumn("host", lower(callUDF("parse_url", col("visit.url"), lit("HOST")))).withColumn("domain", regexp_replace(col("host"), "www.", "")).select(col("uid"),col("visit.timestamp").as("timestamp"), col("domain"))

  val logs_top1000 = logs.groupBy("domain").agg(count("domain").as("top1000")).orderBy(desc("top1000")).limit(1000).withColumnRenamed("domain", "domain_top1000").drop("top1000")

  val logs_2 = logs.join(logs_top1000, logs("domain") === logs_top1000("domain_top1000"), "left")

  val logs_3 = logs_2.groupBy(col("uid")).pivot("domain_top1000").count.na.fill(0).drop("null")

  val logs_4 = logs_3.selectExpr("uid", "array(" + logs_3.drop("uid").schema.fieldNames.map (x => "`"+ x +"`").mkString(",") + ") as domain_features")

  val logs_time = logs.withColumn("timestamp", (col("timestamp")/1000).cast(LongType)).withColumn("web_day", concat(lit("web_day_"), lower(from_unixtime(col("timestamp"),"E")))).withColumn("hour", from_unixtime(col("timestamp"),"HH").cast(IntegerType)).withColumn("web_hour", concat(lit("web_hour_"), col("hour")))

  val logs_time_2 = logs_time.groupBy("uid").agg(count("uid").as("count"), count(when(col("hour") >= 9 && col("hour") < 18, col("hour"))).as("work_hours"), count(when(col("hour") >= 18 && col("hour") < 24, col("hour"))).as("evening_hours")).withColumn("web_fraction_evening_hours", col("evening_hours") / col("count")).withColumn("web_fraction_work_hours", col("work_hours") / col("count")).select(col("uid"), col("web_fraction_work_hours"), col("web_fraction_evening_hours"))

  val logs_time_hour = logs_time.groupBy("uid").pivot("web_hour").count.na.fill(0)

  val logs_time_day = logs_time.groupBy("uid").pivot("web_day").count.na.fill(0)

  val res = logs_4.join(logs_time_hour, Seq("uid"), "left").join(logs_time_day, Seq("uid"), "left").join(logs_time_2, Seq("uid"), "left").join(matrix, Seq("uid"), "left").na.fill(0)

  res.repartition(1).write.mode("overwrite").format("parquet").save("/user/anton.khapinskiy/features")


  spark.stop()

}
