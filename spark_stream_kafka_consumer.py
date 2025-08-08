from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, to_date
from pyspark.sql.types import StructType, StringType, IntegerType

# ---------------------------------------
# 1. Spark Session
# ---------------------------------------
spark = SparkSession.builder \
    .appName("KafkaAirQualityDebug") \
    .master("local[*]") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 👉 (optional but recommended) Ensure timestamp compatibility
spark.conf.set("spark.sql.parquet.writeLegacyFormat", "true")

# ---------------------------------------
# 2. JSON Schema for Parsing
# ---------------------------------------
schema = StructType() \
    .add("city", StringType()) \
    .add("aqi", IntegerType()) \
    .add("main_pollutant", StringType()) \
    .add("ts", StringType())  # Keep ts as STRING for Hive compatibility

# ---------------------------------------
# 3. Kafka Source Stream
# ---------------------------------------
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "air_quality") \
    .option("startingOffsets", "latest") \
    .load()

# ---------------------------------------
# 4. Parse Kafka message & transform
# ---------------------------------------
df_parsed = df_raw \
    .selectExpr("CAST(value AS STRING) AS json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("dt", to_date("ts"))  # extract partition column

# ---------------------------------------
# 5. Write to HDFS in Hive-compatible format (Parquet)
# ---------------------------------------
query = df_parsed.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .trigger(processingTime='15 minutes') \
    .option("checkpointLocation", "/tmp/airquality_checkpoint") \
    .partitionBy("dt") \
    .option("path", "hdfs://localhost:9000/user/hive/warehouse/air_quality.db/air_quality_data") \
    .start()

query.awaitTermination()
