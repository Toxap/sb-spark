import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat_ws, lower, max, regexp_replace}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

object users_items extends App{
  val spark: SparkSession = SparkSession.builder()
    .config("spark.sql.session.timeZone", "UTC")
    .master("local")
    .getOrCreate()

  val update = spark.conf.get("spark.users_items.update")
  val output_dir = spark.conf.get("spark.users_items.output_dir")
  val input_dir = spark.conf.get("spark.users_items.input_dir")

  val schema = new StructType()
    .add("event_type", StringType)
    .add("category", StringType)
    .add("item_id", StringType)
    .add("item_price", IntegerType)
    .add("uid", StringType)
    .add("timestamp", LongType)
    .add("date", StringType)
    .add("p_date", StringType)

  val buyDF = spark
    .read
    .format("json")
    .schema(schema)
    .load(input_dir + "/buy/")

  val buyDF_2 = buyDF
    .withColumn("item_id", regexp_replace(col("item_id"), "-", "_"))
    .withColumn("item_id", regexp_replace(col("item_id"), " ", "_"))
    .withColumn("item_id", lower(col("item_id")))
    .withColumn("item", concat_ws("_", col("event_type"), col("item_id")))
    .drop("category", "event_type", "item_id", "item_price", "timestamp", "p_date")

  val viewDF = spark
    .read
    .format("json")
    .schema(schema)
    .load(input_dir + "/view/")

  val viewDF_2 = viewDF
    .withColumn("item_id", regexp_replace(col("item_id"), "-", "_"))
    .withColumn("item_id", regexp_replace(col("item_id"), " ", "_"))
    .withColumn("item_id", lower(col("item_id")))
    .withColumn("item", concat_ws("_", col("event_type"), col("item_id")))
    .drop("category", "event_type", "item_id", "item_price", "timestamp", "p_date")

  val dfAll = buyDF_2.union(viewDF_2)

  val maxDate = dfAll.agg(max(col("date"))).collect()(0)(0)

  val res = dfAll
    .groupBy(col("uid"))
    .pivot("item")
    .count
    .na.fill(0)

  if (update == 1) {
    val df = spark.read.format("parquet").load(input_dir + "/*")
    df
      .union(res)
      .write
      .format("parquet")
      .mode("overwrite")
      .save(output_dir + "/" + maxDate)
  }
  else {
    res
      .write
      .format("parquet")
      .mode("overwrite")
      .save(output_dir + "/" + maxDate)
  }

}
