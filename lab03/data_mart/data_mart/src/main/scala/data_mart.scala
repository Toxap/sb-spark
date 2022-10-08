
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}



class data_mart extends App{

  val spark: SparkSession = SparkSession.builder()
    .config("spark.cassandra.connection.host", "10.0.0.31")
    .config("spark.cassandra.connection.port", "9042")
    .config("spark.cassandra.output.consistency.level", "ANY")
    .config("spark.cassandra.input.consistency.level", "ONE")
    .getOrCreate()

  import spark.implicits._

  val clients: DataFrame = spark.read
    .format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> "clients", "keyspace" -> "labdata"))
    .load()

  val clients_res = clients
    .withColumn("age_cat", when(col("age") < 25, "18-24")
      .when(col("age") < 35, "25-34")
      .when(col("age") < 45, "35-44")
      .when(col("age") < 55, "45-54")
      .otherwise(">=55"))
    .select("uid", "gender", "age_cat")

  val visits: DataFrame = spark.read
    .format("org.elasticsearch.spark.sql")
    .options(Map("es.read.metadata" -> "true",
      "es.nodes.wan.only" -> "true",
      "es.port" -> "9200",
      "es.nodes" -> "10.0.0.31",
      "es.net.ssl" -> "false"))
    .load("visits")

  val visits_res = visits.withColumn("category", regexp_replace($"category", "-", "_"))
    .withColumn("category", regexp_replace($"category", " ", "_"))
    .withColumn("category", lower($"category"))
    .withColumn("tmp", lit("shop_"))
    .withColumn("shop_category", concat($"tmp", $"category"))
    .select("shop_category", "timestamp", "uid")

  val logs: DataFrame = spark.read.json("hdfs:///labs/laba03/weblogs.json")

  val logs2 = logs.select($"uid",explode($"visits").alias("tmp"))

  val logs3 = logs2.select(col("uid"), col("tmp.*"))
  val logs4 = logs3.toDF("uid", "timestamp", "url")

  val logs5 = logs4
    .withColumn("domain", regexp_extract(col("url"), raw"^(?:https?:\/\/)?(?:[^@\/\n]+@)?(?:www\.)?([^:\/?\n]+)", 1))
    .select("uid", "timestamp", "domain")

  val cats: DataFrame = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://10.0.0.31:5432/labdata")
    .option("dbtable", "domain_cats")
    .option("user", "anton_khapinskiy")
    .option("password", "6VMpn6e0")
    .option("driver", "org.postgresql.Driver")
    .load()

  val cats2 = cats
    .withColumn("tmp", lit("web_"))
    .withColumn("web_category", concat($"tmp", $"category"))
    .select("domain", "web_category")

  val res1 = clients_res.join(visits_res, Seq("uid"), "left")

  val res2 = res1.groupBy("uid", "gender", "age_cat").pivot("shop_category").count()

  val res3 = res2.drop(col("null"))

  val res_1 = clients_res.join(logs5, Seq("uid"), "left").join(cats2, Seq("domain"), "left")

  val res_2 = res_1.groupBy("uid", "gender", "age_cat").pivot("web_category").count()

  val res_3 = res_2.drop(col("null"))

  val res = res3.join(res_3, Seq("uid", "gender", "age_cat"), "inner")

  val res_end = res.na.fill(0)

  res_end.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://10.0.0.31:5432/anton_khapinskiy")
    .option("dbtable", "clients")
    .option("user", "anton_khapinskiy")
    .option("password", "6VMpn6e0")
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()



}
