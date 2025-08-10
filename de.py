import logging
from pyspark.sql import SparkSession
from kafka import KafkaProducer
import json
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("KafkaProducerLogger")

# Spark session with Excel support
spark = SparkSession.builder \
    .appName("IMDB Movies Producer with Validation") \
    .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:3.5.0_0.18.7") \
    .getOrCreate()

# Read Excel directly
movies_df = spark.read \
    .format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("movies.xlsx")

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Validation function
def is_valid(record):
    try:
        if not record.get("id") or not isinstance(record["id"], int):
            return False
        if not record.get("title") or str(record["title"]).strip() == "":
            return False
        if record.get("vote_average") is not None and not (0 <= record["vote_average"] <= 10):
            return False
        if record.get("vote_count") is not None and record["vote_count"] < 0:
            return False
        if record.get("budget") is not None and record["budget"] < 0:
            return False
        if record.get("revenue") is not None and record["revenue"] < 0:
            return False
        if record.get("release_date"):
            try:
                datetime.strptime(str(record["release_date"]), "%Y-%m-%d")
            except ValueError:
                return False
        return True
    except Exception:
        return False

# Get first 150 records
rows = movies_df.limit(150).collect()

# Send only valid records
valid_count = 0
invalid_count = 0

for idx, row in enumerate(rows, start=1):
    record = row.asDict()
    if is_valid(record):
        future = producer.send("movie-ratings", value=record)
        try:
            result = future.get(timeout=10)
            valid_count += 1
            logger.info(f"✅ Sent record {idx}/{len(rows)} to 'movie-ratings' (Partition {result.partition}, Offset {result.offset})")
        except Exception as e:
            logger.error(f"❌ Failed to send record {idx}: {e}")
    else:
        invalid_count += 1
        logger.warning(f"⚠ Skipped invalid record {idx}: {record}")

producer.flush()
logger.info(f"🎯 Finished sending {valid_count} valid records, skipped {invalid_count} invalid records.")
