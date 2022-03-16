import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_timestamp, substring, to_timestamp, unix_timestamp}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config(ConfigurationOptions.ES_NODES, "127.0.0.1")
      .config(ConfigurationOptions.ES_PORT, "9200")
      .appName( name = "STREAM_APP")
      .master( master = "local[*]")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    val schema = StructType(
      Array(StructField("protocol", StringType),
        StructField("HTTP_status", StringType),
        StructField("URL", StringType),
        StructField("path", StringType),
        StructField("IP", StringType),
        StructField("timestamp",TimestampType)))

    val StreamDF = spark.readStream.option("delimiter",",").schema(schema)
      .csv("/home/dba/Projet_Scala/data")
      .withColumn("time_stamp", (unix_timestamp($"timestamp", "yyyy-MM-dd HH:mm:ss.SSS") +
        substring($"timestamp", -6, 6).cast("float")/1000000)
        .cast(TimestampType))
      .drop("timestamp")

    StreamDF.createOrReplaceTempView( "SDF")
    val outDF = spark.sql( "select * from SDF")
    outDF
      .writeStream
      .format("org.elasticsearch.spark.sql")
      .option("checkpointLocation", "checkpoint")
      .outputMode( "append")
      .start("index-2/logs-1")
      .awaitTermination()
   }
}
