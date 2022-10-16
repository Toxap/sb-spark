import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType,StringType,StructType,TimestampType, LongType}


object filter extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .getOrCreate()


  val offset: String = spark.conf.get("spark.filter.offset")
  val topic_name: String = spark.conf.get("spark.filter.topic_name")
  val output_dir_prefix: String = spark.conf.get("spark.filter.output_dir_prefix")

  val kafkaDF = spark.read.format("kafka")
    .option("kafka.bootstrap.servers", "10.0.0.31:6667")
    .option("subscribe", topic_name)
    .option("startingOffsets",
      if (offset.contains("earliest"))
        offset
      else {
        "{\"" + topic_name + "\":{\"0\":" + offset + "}}"
      }
    )
    .load()
  spark.conf.set("spark.sql.session.timeZone", "UTC")

  val personStringDF = kafkaDF.selectExpr("CAST(value AS STRING)")

  val schema = new StructType()
    .add("event_type", StringType)
    .add("category", StringType)
    .add("item_id", StringType)
    .add("item_price", IntegerType)
    .add("uid", StringType)
    .add("timestamp", LongType)

  val personDF = personStringDF.select(from_json(col("value"), schema).as("data"))
    .select("data.*")

  val dateDF = personDF
    .withColumn("p_date", to_date((col("timestamp") / 1000).cast(TimestampType)))
    .withColumn("p_date", date_format(col("p_date"), "yyyyMMdd"))

  dateDF
    .filter(col("event_type") === "view")
    .write
    .mode("overwrite")
    .partitionBy("p_date")
    .json(output_dir_prefix + "/view")

  dateDF
    .filter(col("event_type") === "buy")
    .write
    .mode("overwrite")
    .partitionBy("p_date")
    .json(output_dir_prefix + "/buy")
}