# 🌦️ **Weather Data Streaming Pipeline**

This project implements a **real-time data pipeline** that simulates synthetic weather data, streams it through **Apache Kafka**, processes it using **Apache Spark**, and stores the transformed output in **Apache Cassandra**.  
The entire pipeline is automated and orchestrated using **Apache Airflow**, enabling end-to-end scheduling, monitoring, and scalability.

## 📌 **Overview**

This project focuses on building a **robust and scalable real-time data pipeline** that simulates live weather conditions and processes them through a modern big data stack.

The pipeline captures synthetic weather data, streams it into Kafka, processes and flattens the nested structure using Apache Spark, stores the refined data in Cassandra, and automates the workflow using Apache Airflow.

---

## 🚀 **Core Highlights**

- 🔄 **Live Weather Data Simulation**  
  Generates continuous weather metrics using Python’s `Faker` library to mimic real-world patterns.

- 📡 **Kafka Integration**  
  Publishes nested JSON messages to Kafka topics for high-throughput, fault-tolerant streaming.

- ⚙️ **Spark Streaming**  
  Reads data from Kafka, transforms and flattens the JSON structure, making it ready for storage.

- 🗄️ **Cassandra Storage**  
  Writes the processed data into a Cassandra database for efficient querying and high availability.

- 🪄 **Airflow Orchestration**  
  Coordinates and schedules each stage of the pipeline to ensure smooth and automated execution.

## 🧰 Tech Stack

- **Apache Kafka** – For real-time data streaming
- **Apache Spark** – For stream processing and data transformation
- **Apache Cassandra** – For scalable data storage
- **Apache Airflow** – For orchestration and automation
- **Docker** – For containerized deployments
- **Python (Faker)** – For generating synthetic weather data
