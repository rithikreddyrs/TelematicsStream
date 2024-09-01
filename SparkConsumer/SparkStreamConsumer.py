from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lag
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DecimalType, NumericType, UserDefinedType, TimestampType, LongType
from pyspark.sql.window import Window
# Spark session creation
spark = SparkSession.builder \
    .appName("Kafka Spark Consumer") \
    .config("spark.hadoop.fs.s3a.access.key", "update-accesskey") \
    .config("spark.hadoop.fs.s3a.secret.key", "update-secretkey") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")

# Kafka configuration
kafka_bootstrap_servers = 'localhost:9092'
kafka_topics = ['GPS_Topic', 'Speed_Topic', 'Acceleration_Topic']

# Define the schema for GPS_Topic
gps_schema = StructType([
    StructField("Vendor ID", StringType(), True),
    StructField("IMEI/Device Serial no", StringType(), True),
    StructField("Date", FloatType(), True),
    StructField("Time and zone", TimestampType(), True),
    StructField("GPS Fix", FloatType(), True),
    StructField("Latitude", FloatType(), True),
    StructField("Latitude Direction", StringType(), True),
    StructField("Longitude", FloatType(), True),
    StructField("Longitude Direction", StringType(), True),
    StructField("Heading",FloatType(),True)
])

# Define the schema for Speed_Topic
speed_schema = StructType([
    StructField("Vendor ID", StringType(), True),
    StructField("IMEI/Device Serial no", StringType(), True),
    StructField("Date", FloatType(), True),
    StructField("Time and zone", TimestampType(), True),
    StructField("Speed", FloatType(), True),
    StructField("ignition_status", FloatType(),True)
])

# Define the schema for Acceleration_Topic
acceleration_schema = StructType([
    StructField("Vendor ID", StringType(), True),
    StructField("IMEI/Device Serial no", StringType(), True),
    StructField("Date", FloatType(), True),
    StructField("Time and zone", TimestampType(), True),
    StructField("acceleration", FloatType(), True)
])

# Function to read from Kafka and parse JSON messages
def read_kafka_topic(topic):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load() \

def renameColumns(df):
    columns=df.columns
    new_columns=[str(column).lower().replace(" ","_") for column in columns]
    df=df.toDF(*new_columns)
    return df
# Creating DataFrames for each topic
gps_df = read_kafka_topic("GPS_Topic")
gps_df.printSchema()
gps_df=gps_df.select(from_json(col("value").cast("string"), gps_schema).alias("data")) \
        .select("data.*")
gps_df.printSchema()
speed_df = read_kafka_topic("Speed_Topic")
speed_df.printSchema()
speed_df=speed_df.select(from_json(col("value").cast("string"), speed_schema).alias("data")) \
        .select("data.*")
speed_df.printSchema()
acceleration_df = read_kafka_topic("Acceleration_Topic")
acceleration_df.printSchema()
acceleration_df=acceleration_df.select(from_json(col("value").cast("string"), acceleration_schema).alias("data")) \
        .select("data.*")
acceleration_df.printSchema()

gps_df=renameColumns(gps_df)
speed_df=renameColumns(speed_df)
acceleration_df=renameColumns(acceleration_df)

gps_df=gps_df.dropna()
speed_df=speed_df.dropna()
acceleration_df=acceleration_df.dropna()

gps_df=gps_df.dropna(subset=["gps_fix"])
speed_df=speed_df.dropna(subset=["speed"])
acceleration_df=acceleration_df.dropna(subset=["acceleration"])

#Harsh breaking threashold 3
threashold=3


harsh_braking_threshold = -3.0  #threshold value
acceleration_df = acceleration_df.withColumn("harsh_braking", col("acceleration") < harsh_braking_threshold)
#Speed limit 30
speed_limit=30
speed_df=speed_df.withColumn("speed_limit_exceed", col('speed')>speed_limit)
#Ignition on
speed_df=speed_df.withColumn("idle_engine",((col('speed')==0) & (col('ignition_status')!=0)))
#Right turn
gps_df=gps_df.withColumn("right_turn",((col('heading')>70) | (col('heading')<100)))

query_gps = gps_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query_speed = speed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query_acceleration = acceleration_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()



query_gps_s3 = gps_df.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "s3a://spark-stream-rgr/checkpoint/gps-data/") \
    .option("path", "s3a://spark-stream-rgr/gps-data/") \
    .outputMode("append") \
    .start()

query_speed_s3 = speed_df.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "s3a://spark-stream-rgr/checkpoint/speed-data/") \
    .option("path", "s3a://spark-stream-rgr/speed-data/") \
    .outputMode("append") \
    .start()

query_acceleration_s3 = acceleration_df.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "s3a://spark-stream-rgr/checkpoint/acceleration-data") \
    .option("path", "s3a://spark-stream-rgr/acceleration-data/") \
    .outputMode("append") \
    .start()

# Write the streaming DataFrame to Delta Lake
query_gps_delta=gps_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://spark-stream-rgr/checkpoint/deltalake/gps-data/") \
    .start("s3a://spark-stream-rgr/deltalake/gps_data/")
query_speed_delta=speed_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://spark-stream-rgr/checkpoint/deltalake/speed-data/") \
    .start("s3a://spark-stream-rgr/deltalake/speed_data/")
query_acceleration_delta=acceleration_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://spark-stream-rgr/checkpoint/deltalake/acceleration-data/") \
    .start("s3a://spark-stream-rgr/deltalake/acceleration_data/")
# Wait for the streams to finish
query_gps.awaitTermination()
query_speed.awaitTermination()
query_acceleration.awaitTermination()
query_gps_s3.awaitTermination()
query_acceleration_s3.awaitTermination()
query_speed_s3.awaitTermination()
query_gps_delta.awaitTermination()
query_acceleration_delta.awaitTermination()
query_speed_delta.awaitTermination()


