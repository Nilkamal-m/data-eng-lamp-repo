from datetime import timedelta
import time
import random
import json
import logging
from kafka import KafkaProducer
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import numpy as np
from faker import Faker

default_args = {
    "owner": "Nilkamal Mahato",
    "start_date": days_ago(1),
    "email": ["nilkamal.35@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True
}

fake = Faker()
logging.basicConfig(level=logging.INFO)

records_per_second = 100
run_for_seconds = 540
KAFKA_HOSTS = ["kafka:9092"]


def generate_weather_data():
    temperature = np.random.normal(loc=15, scale=10)
    humidity = np.random.normal(loc=60, scale=20)
    pressure = np.random.normal(loc=1013, scale=10)
    wind_speed = np.random.exponential(scale=5)
    wind_direction = random.choice(
        ["N", "NE", "E", "SE", "S", "SW", "W", "NW"])
    temperature = max(min(temperature, 40), -20)
    humidity = max(min(humidity, 100), 0)
    pressure = max(min(pressure, 1050), 950)
    wind_speed = round(min(wind_speed, 100), 1)

    return {
        "station_id": fake.uuid4(),
        "timestamp": fake.iso8601(),
        "location": {
            "city": fake.city(),
            "country": fake.country(),
            "latitude": float(fake.latitude()),
            "longitude": float(fake.longitude()),
        },
        "temperature": round(temperature, 1),
        "humidity": int(humidity),
        "pressure": round(pressure, 1),
        "wind_speed": wind_speed,
        "wind_direction": wind_direction,
        "precipitation": round(random.uniform(0, 50), 1),
        "weather_condition": random.choice(["Sunny", "Cloudy", "Rainy", "Snowy", "Stormy"]),
    }


def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_HOSTS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        max_block_ms=5000,
        linger_ms=100,
        retries=5,
        acks="all",
    )


def stream_weather_data(records_per_second, run_for_seconds):
    producer = None
    try:
        producer = create_kafka_producer()
        logging.info(
            "Started streaming weather data to Kafka topic 'weather_data'")
        start_time = time.time()
        while True:
            if run_for_seconds and time.time() - start_time >= run_for_seconds:
                break
            for _ in range(records_per_second):
                weather_data = generate_weather_data()
                future = producer.send("weather_data", value=weather_data)
                future.get(timeout=10)
            time.sleep(1)
        logging.info("Successfully completed data streaming to Kafka.")
    except Exception as e:
        logging.error(f"Error occurred while streaming data to Kafka: {e}")
        raise
    finally:
        if producer:
            producer.flush()
            producer.close(timeout=60)
            logging.info("Kafka producer closed.")


with DAG(
    "weather_data_streaming",
    default_args=default_args,
    schedule_interval=timedelta(minutes=10),
    catchup=False,
    max_active_runs=1,
) as dag:

    kafka_streaming_task = PythonOperator(
        task_id="stream_weather_data_kafka",
        python_callable=stream_weather_data,
        op_kwargs={
            "records_per_second": records_per_second,
            "run_for_seconds": run_for_seconds,
        },
        execution_timeout=timedelta(minutes=15),
    )
