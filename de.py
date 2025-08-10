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


import logging
from kafka import KafkaConsumer
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("KafkaConsumerLogger")

# Create Kafka consumer
consumer = KafkaConsumer(
    'movie-ratings',
    bootstrap_servers=['localhost:9092'],  # Change if Kafka broker is remote
    auto_offset_reset='earliest',          # Read from beginning
    enable_auto_commit=True,               # Commit offsets automatically
    group_id="movie-ratings-group",        # Consumer group ID
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))  # Parse JSON
)

logger.info("📡 Consumer started. Reading messages from 'movie-ratings'...")

# Poll for messages
try:
    for message in consumer:
        record = message.value  # Already parsed JSON
        logger.info(f"📄 Offset={message.offset}, Partition={message.partition}, Key={message.key}, Record Title={record.get('title')}")
        # Example: Access specific fields
        # print(record["title"], record["vote_average"])
except KeyboardInterrupt:
    logger.info("🛑 Consumer stopped manually.")
finally:
    consumer.close()



import logging
from kafka import KafkaConsumer
import json
import csv
import os
import time
from datetime import datetime

# Logging config
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("KafkaConsumerLogger")

# Base file names
english_csv_base = "english_movies"
war_csv_base = "war_movies"
all_csv_base = "all_movies"

# File size limit in bytes (5 MB)
MAX_FILE_SIZE = 5 * 1024 * 1024
MIN_ROWS = 20

def get_csv_filename(base_name):
    """Returns the active CSV file name."""
    return f"{base_name}.csv"

def rotate_file(base_name):
    """Renames file with timestamp and returns new file path."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    old_file = get_csv_filename(base_name)
    new_file = f"{base_name}_{timestamp}.csv"
    os.rename(old_file, new_file)
    logger.warning(f"📂 Rotated file: {old_file} -> {new_file}")

def check_and_rotate(base_name):
    """Check file size and rotate if too big."""
    file_path = get_csv_filename(base_name)
    if os.path.isfile(file_path) and os.path.getsize(file_path) > MAX_FILE_SIZE:
        rotate_file(base_name)

def validate_min_rows(file_path):
    """Check if CSV has at least MIN_ROWS rows (excluding header)."""
    if os.path.isfile(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            rows = sum(1 for _ in f) - 1  # subtract header
        if rows < MIN_ROWS:
            logger.warning(f"⚠️ {file_path} has only {rows} rows (< {MIN_ROWS})")
        else:
            logger.info(f"✅ {file_path} has {rows} rows (OK)")

# Kafka consumer
consumer = KafkaConsumer(
    'movie-ratings',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id="movie-ratings-group",
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

logger.info("📡 Consumer started. Reading messages from 'movie-ratings'...")

try:
    # Open files in append mode
    with open(get_csv_filename(english_csv_base), 'a', newline='', encoding='utf-8') as f_en, \
         open(get_csv_filename(war_csv_base), 'a', newline='', encoding='utf-8') as f_war, \
         open(get_csv_filename(all_csv_base), 'a', newline='', encoding='utf-8') as f_all:

        english_writer = None
        war_writer = None
        all_writer = None

        for message in consumer:
            record = message.value

            # Rotate files if needed
            check_and_rotate(english_csv_base)
            check_and_rotate(war_csv_base)
            check_and_rotate(all_csv_base)

            # Write ALL movies
            if all_writer is None:
                all_writer = csv.DictWriter(f_all, fieldnames=record.keys())
                if f_all.tell() == 0:  # file empty
                    all_writer.writeheader()
            all_writer.writerow(record)
            logger.info(f"💾 Saved to all_movies.csv: {record.get('title')}")

            # Write English movies
            if record.get("original_language") == "en":
                if english_writer is None:
                    english_writer = csv.DictWriter(f_en, fieldnames=record.keys())
                    if f_en.tell() == 0:
                        english_writer.writeheader()
                english_writer.writerow(record)
                logger.info(f"✅ Saved English: {record.get('title')}")

            # Write War movies
            keywords = str(record.get("keywords", "")).lower()
            if "war" in keywords:
                if war_writer is None:
                    war_writer = csv.DictWriter(f_war, fieldnames=record.keys())
                    if f_war.tell() == 0:
                        war_writer.writeheader()
                war_writer.writerow(record)
                logger.info(f"🔥 Saved War: {record.get('title')}")

            # Periodic validation
            validate_min_rows(get_csv_filename(english_csv_base))
            validate_min_rows(get_csv_filename(war_csv_base))
            validate_min_rows(get_csv_filename(all_csv_base))

except KeyboardInterrupt:
    logger.info("🛑 Consumer stopped manually.")
finally:
    consumer.close()

