from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StringType, IntegerType

spark = SparkSession.builder \
    .appName("KafkaToHiveStreaming") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")  # Optional: reduce log noise

schema = StructType() \
    .add("city", StringType()) \
    .add("aqi", IntegerType()) \
    .add("main_pollutant", StringType()) \
    .add("ts", StringType())  # will be converted to timestamp

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "air_quality") \
    .option("startingOffsets", "latest") \
    .load()

json_df = raw_df.selectExpr("CAST(value AS STRING) AS json_str")

parsed_df = json_df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")


final_df = parsed_df.withColumn("ts", to_timestamp(col("ts")))

query = final_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "hdfs://localhost:9000/user/hive/warehouse/air_quality.db/air_quality_data")\
    .option("checkpointLocation", "hdfs://localhost:9000/tmp/kafka_to_hive_checkpoint") \
    .start()

query.awaitTermination()

