# 🌦️ Real-Time Weather Data Streaming Pipeline

A real-time data streaming pipeline using **Kafka**, **Apache Spark Structured Streaming**, **Apache Cassandra**, and **Apache Airflow**, designed to simulate and process synthetic weather data in a Dockerized environment.

## 🔧 Tech Stack

- **Apache Kafka** - Messaging queue to stream weather data.
- **Apache Spark** - Real-time data processing and transformation.
- **Apache Cassandra** - NoSQL database to store weather data.
- **Apache Airflow** - Task scheduling for the Kafka producer.
- **Docker Compose** - Container orchestration for all services.
- **Confluent Control Center** - Kafka UI monitoring tool.
- **PostgreSQL** - Metadata DB for Airflow.

## 📦 Project Structure

```
.
├── dags/
│   └── weather_data_streaming.py      # Airflow DAG to stream data to Kafka
├── spark_script/
│   └── spark_stream_consumer.py       # Spark Structured Streaming consumer
├── script/
│   └── entrypoint.sh                  # Custom entrypoint for Airflow webserver
├── docker-compose.yml                 # Full stack orchestration
├── requirements.txt                   # Python dependencies for Airflow
└── README.md                          # This file
```

## ⚙️ How It Works

### ➤ Data Generation & Publishing

- `weather_data_streaming.py` DAG generates realistic weather data using `faker` and `numpy`.
- It sends data to Kafka topic `weather_data` at a fixed rate (`records_per_second`).

### ➤ Data Consumption & Processing

- `spark_stream_consumer.py` consumes Kafka topic using Spark Structured Streaming.
- It flattens nested JSON, parses the schema, and writes the data to Cassandra.

### ➤ Orchestration & Deployment

- Services are containerized using Docker Compose.
- Airflow scheduler and webserver manage the Kafka data generation DAG.
- Spark master/worker nodes handle the consumer logic.
- Cassandra persists the processed weather data.

---

## 🚀 Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/Nilkamal-m/data-eng-lamp-repo.git
cd data-eng-lamp-repo
```

### 2. Start the Stack

```bash
docker-compose up --build -d
```

### 3. Access the Services

- **Airflow UI** → [http://localhost:8080](http://localhost:8080)
  - Username: `admin`, Password: `admin@123`
- **Kafka Control Center** → [http://localhost:9021](http://localhost:9021)
- **Schema Registry** → [http://localhost:8081](http://localhost:8081)
- **Spark UI** → [http://localhost:9099](http://localhost:9099)

### 4. Trigger the Kafka Streaming DAG

From Airflow UI, manually run the DAG `weather_data_streaming`. It will publish data to Kafka.

### 5. Run the Spark Consumer

```bash
docker exec -it spark-master bash
cd /opt/bitnami/spark/apps
spark-submit \
  --packages com.datastax.spark:spark-cassandra-connector_2.13:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
  --conf spark.jars.ivy="/tmp" \
  --conf spark.cassandra.connection.host=cassandra \
  --conf spark.cassandra.connection.port=9042 \
  --conf spark.driver.memory=4g \
  spark_stream_comsumer.py
```

---

## 🧪 Example Weather Data Payload

```json
{
  "station_id": "79ba2d8f-748a-42fc-b31e-b50030cb94bc",
  "timestamp": "1973-10-29T07:50:33.417959",
  "location": {
    "city": "New Alexis",
    "country": "Fiji",
    "latitude": 83.1585015,
    "longitude": 93.406214
  },
  "temperature": 11.5,
  "humidity": 35,
  "pressure": 1024,
  "wind_speed": 0.2,
  "wind_direction": "NE",
  "precipitation": 10.1,
  "weather_condition": "Cloudy"
}
```

---

## 📂 Cassandra Schema

```cql
CREATE KEYSPACE IF NOT EXISTS spark_streams
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

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
```

---

## 🛠️ Troubleshooting

- Kafka topic not found? Ensure `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`.
- Cassandra errors? Wait 30–60 sec for Cassandra to fully initialize.
- Spark not connecting to Kafka? Verify `.jar` packages are correctly configured.

---

## 🙌 Acknowledgements

- Apache Kafka
- Apache Spark
- Apache Cassandra
- Apache Airflow
- Docker

---

## 📧 Contact

Created by **Nilkamal Mahato** – [nilkamal.35@gmail.com](mailto:nilkamal.35@gmail.com)  
Feel free to reach out for questions, feedback, or collaborations!
