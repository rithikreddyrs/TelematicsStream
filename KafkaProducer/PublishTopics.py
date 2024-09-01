from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, from_unixtime, lit, lag
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType
from confluent_kafka import Producer
import json

# Kafka configuration
kafka_bootstrap_servers = 'localhost:9092'
KAFKA_CONFIG = {
    'bootstrap.servers': kafka_bootstrap_servers
}
# Create Kafka producers for different topics
producers = {
    'GPS_Topic': Producer(KAFKA_CONFIG),
    'Speed_Topic': Producer(KAFKA_CONFIG),
    'Acceleration_Topic': Producer(KAFKA_CONFIG)
}


# Function to send messages to Kafka
def send_to_kafka(topic, key, value):
    producers[topic].produce(topic, key=str(key), value=json.dumps(value))
    producers[topic].flush()


# Function to read Excel file
def read_excel(file_path):
    return spark.read.format("com.crealytics.spark.excel") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(file_path)


# Function to calculate acceleration
def calc_acceleration(df):
    # Define a window specification, ordering by the 'Time and zone' column
    windowSpec = Window.orderBy("Time and zone").partitionBy("Vendor ID")

    # Use the lag function to calculate the previous 'Time and zone' value
    df = df.withColumn("prev_time", lag(col("Time and zone")).over(windowSpec))
    # Calculate the time difference (assuming 'Time and zone' is in seconds or another integer format)
    df = df.withColumn("time_diff", col("Time and zone") - col("prev_time"))
    df = df.withColumn("prev_speed", lag(col("Speed")).over(windowSpec))
    df = df.withColumn("speed_diff", col("Speed") - col("prev_speed"))

    df = df.withColumn("acceleration", col("speed_diff") / col("time_diff"))
    return df


# Function to convert seconds to HH:mm:ss
def convert_seconds_to_time(seconds):
    return from_unixtime(seconds.cast(IntegerType()), 'HH:mm:ss')


# Spark session creation
spark = SparkSession.builder \
    .appName("Spark Kafka Integration") \
    .getOrCreate()

# Read the Excel file into Spark DataFrame
file_path = 'C:\\Users\\rithi\\Downloads\\35541211927778616jan.xlsx'  # Update this path
df = read_excel(file_path)

# Calculate acceleration
df = calc_acceleration(df)

# Convert Time and zone to time format
df = df.withColumn("Time and zone", convert_seconds_to_time(col("Time and zone")))

# Select relevant columns and send to Kafka
gps_df = df.select("Vendor ID", "IMEI/Device Serial no", "Date", "Time and zone",
                   "GPS Fix", "Latitude", "Latitude Direction", "Longitude", "Longitude Direction", "Heading")
speed_df = df.select("Vendor ID", "IMEI/Device Serial no", "Date", "Time and zone", "Speed", "_c21")
speed_df = speed_df.withColumnRenamed("_c21", "ignition_status")
acceleration_df = df.select("Vendor ID", "IMEI/Device Serial no", "Date", "Time and zone", "acceleration")
# Send to Kafka
for row in gps_df.collect():
    send_to_kafka('GPS_Topic', row['Vendor ID'], row.asDict())

for row in speed_df.collect():
    send_to_kafka('Speed_Topic', row['Vendor ID'], row.asDict())

for row in acceleration_df.collect():
    send_to_kafka('Acceleration_Topic', row['Vendor ID'], row.asDict())
