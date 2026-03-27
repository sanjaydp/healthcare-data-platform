from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType

KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "encounter_events"
CHECKPOINT = "/tmp/checkpoints/encounter_events"
OUTPUT_PATH = "/opt/project/data/bronze/encounter_events"

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_ts", StringType(), True),
    StructField("payload", StringType(), True),
])

spark = SparkSession.builder \
    .appName("KafkaStream") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

json_df = raw_df.selectExpr("CAST(value AS STRING) as json_value")

parsed_df = json_df.select(
    from_json(col("json_value"), schema).alias("data")
).select("data.*")

query = (
    parsed_df.writeStream
    .format("json")
    .option("path", OUTPUT_PATH)
    .option("checkpointLocation", CHECKPOINT)
    .outputMode("append")
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()