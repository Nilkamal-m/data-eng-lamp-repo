import logging
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    IntegerType,
    TimestampType
)

logging.basicConfig(level=logging.INFO)


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    logging.info("✅ Cassandra keyspace created or exists.")


def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.weather_data (
            station_id UUID PRIMARY KEY,
            timestamp TEXT,
            city TEXT,
            country TEXT,
            latitude FLOAT,
            longitude FLOAT,
            temperature FLOAT,
            humidity INT,
            pressure FLOAT,
            wind_speed FLOAT,
            wind_direction TEXT,
            precipitation FLOAT,
            weather_condition TEXT
        );
    """)
    logging.info("✅ Cassandra table created or exists.")


def create_spark_connection():
    try:
        spark = (
            SparkSession.builder.appName("SparkWeatherDataStreaming")
            .config(
                "spark.jars.packages",
                "com.datastax.spark:spark-cassandra-connector_2.12:3.4"
                "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0"
            )
            .config("spark.cassandra.connection.host", "cassandra")
            .config("spark.driver.memory", "4g")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        logging.info("✅ Spark session established.")
        return spark
    except Exception as e:
        logging.error(f"❌ Failed to create Spark session: {e}")
        return None


def connect_to_kafka(spark):
    try:
        df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "kafka:9092")
            .option("subscribe", "weather_data")
            .option("startingOffsets", "earliest")
            .load()
        )
        logging.info("✅ Connected to Kafka topic 'weather_data'")
        return df
    except Exception as e:
        logging.error(f"❌ Could not connect to Kafka: {e}")
        return None


def create_cassandra_connection():
    try:
        auth_provider = PlainTextAuthProvider('cassandra', 'cassandra')
        cluster = Cluster(["cassandra"], port=9042,
                          auth_provider=auth_provider)
        session = cluster.connect()
        logging.info("✅ Connected to Cassandra")
        return session
    except Exception as e:
        logging.error(f"❌ Cassandra connection failed: {e}")
        return None


def create_selection_df_from_kafka(kafka_df):
    schema = StructType([
        StructField("station_id", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("location", StructType([
            StructField("city", StringType(), False),
            StructField("country", StringType(), False),
            StructField("latitude", FloatType(), False),
            StructField("longitude", FloatType(), False),
        ])),
        StructField("temperature", FloatType(), False),
        StructField("humidity", IntegerType(), False),
        StructField("pressure", FloatType(), False),
        StructField("wind_speed", FloatType(), False),
        StructField("wind_direction", StringType(), False),
        StructField("precipitation", FloatType(), False),
        StructField("weather_condition", StringType(), False),
    ])

    parsed_df = kafka_df.selectExpr("CAST(value AS STRING)").select(
        from_json(col("value"), schema).alias("data")
    )

    flat_df = parsed_df.select(
        col("data.station_id"),
        col("data.timestamp"),
        col("data.location.city").alias("city"),
        col("data.location.country").alias("country"),
        col("data.location.latitude").alias("latitude"),
        col("data.location.longitude").alias("longitude"),
        col("data.temperature"),
        col("data.humidity"),
        col("data.pressure"),
        col("data.wind_speed"),
        col("data.wind_direction"),
        col("data.precipitation"),
        col("data.weather_condition"),
    )

    return flat_df


def start_spark_stream():
    spark = create_spark_connection()
    if spark is None:
        return

    kafka_df = connect_to_kafka(spark)
    if kafka_df is None:
        return

    selection_df = create_selection_df_from_kafka(kafka_df)

    session = create_cassandra_connection()
    if session is None:
        return

    create_keyspace(session)
    session.set_keyspace("spark_streams")
    create_table(session)

    logging.info("🚀 Starting the streaming job...")

    try:
        query = (
            selection_df.writeStream
            .format("org.apache.spark.sql.cassandra")
            .option("checkpointLocation", "/tmp/checkpoint")
            .option("keyspace", "spark_streams")
            .option("table", "weather_data")
            .start()
        )
        logging.info("✅ Streaming job started. Waiting for termination...")
        query.awaitTermination()
    except Exception as e:
        logging.error(f"❌ Streaming failed: {e}")


if __name__ == "__main__":
    start_spark_stream()
