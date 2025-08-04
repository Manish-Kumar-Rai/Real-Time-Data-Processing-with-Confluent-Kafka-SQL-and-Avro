# 🛰️ Real-Time Data Processing with Confluent Kafka, SQL, and Avro

This project demonstrates a real-time data processing pipeline using:
- **Microsoft SQL Server** as the source database
- **Confluent Kafka** as the streaming platform
- **Avro** as the serialization format
- **Python** for producer/consumer implementation

---

## 📁 Project Structure

```
Real-Time-Data-Processing-with-Confluent-Kafka-SQL-and-Avro/
│
├── venv/                       # Python virtual environment (excluded from Git)
├── .env                        # Environment variables (DB credentials, Kafka configs)
├── .gitignore                  # Git ignore rules (venv/, __pycache__/, .env, etc.)
├── README.md                   # You're here!
├── requirements.txt            # Python dependencies
│
├── producer/                   # Kafka producer logic
│   ├── kafka_producer.py       # Sends records to Kafka topic in Avro format
│   ├── mssql_connector.py      # Connects to MSSQL and fetches new records
│   └── avro_schema.avsc        # Avro schema for serialization
│   └── checkpoints             # Directory to save last checkpoint 
│
├── consumer/                   # Kafka consumer logic
│   ├── kafka_consumer.py       # Consumer group (5 members)
│   ├── data_transform.py       # Business logic on consumed data
│   └── json_writer.py          # Writes transformed output as NDJSON
│   └── kafka_output            # Directory to save consumer outputs
│
├── config/                     # Configuration files
│   └── config.yaml             # YAML for Kafka, DB, and file paths
│   └── logger.py               # common logger file 
│
├── images/                     #Screenshot of working project
│   └── screenshot.png          
├── scripts/                    # SQL scripts for setup
│   ├── create_table.sql        # Create product table
│   └── sample_inserts.sql      # Insert test records into table
```

---

## ⚙️ Setup Instructions

### 1. 🧱 Environment Setup
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. 📦 Install Confluent Tools
Install Confluent Kafka + Schema Registry locally via Docker or manual setup.

- You must have these running:
  - Kafka broker
  - Zookeeper
  - Schema Registry

Refer: [https://docs.confluent.io/platform/current/quickstart/index.html](https://docs.confluent.io/platform/current/quickstart/index.html)

### 3. 🗃️ SQL Server Setup
- Create the `products` table:
```bash
scripts/create_table.sql
```
- Insert test data:
```bash
scripts/sample_inserts.sql
```

### 4. 🔐 Add Environment Variables
Create a `.env` file with the following:
```env
# MSSQL Database Configuration
DB_SERVER=your_server_name
DB_NAME=your_database_name
DB_USER=sa
DB_PASSWORD=your_db_password_here  # Replace with your actual password

# Confluent Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=your_bootstrap_server_link
KAFKA_SASL_MECHANISMS=PLAIN
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_USERNAME=your_kafka_username_here
KAFKA_PASSWORD=your_kafka_password_here

# Schema Registry Configuration
KAFKA_SCHEMA_REGISTRY_URL=your_registry_url
KAFKA_SCHEMA_REGISTRY_USERNAME=your_registry_username_here
KAFKA_SCHEMA_REGISTRY_PASSWORD=your_registry_password_here
```

---

### 🚀 CLI Commands to Run the Kafka Pipeline

```bash
# 1️⃣ Insert sample data into the SQL Server table
python -m sqlScripts.insert_sample

# 2️⃣ Run the Kafka Producer to fetch from SQL and push to Kafka
python -m kafka_producer.kafka_producer

# 3️⃣ Start the Kafka Consumer to read, transform, and store the data
python -m kafka_consumer.kafka_consumer

---

## 📘 Key Concepts

- **Incremental Fetching:** Fetch only new SQL rows using `last_updated > ?`
- **Avro Serialization:** Schema-based efficient messaging format
- **Kafka Consumer Group:** 5 consumers load-balanced across topic partitions
- **Business Logic Layer:** Apply transformations before storage
- **NDJSON Output:** JSON format with one record per line

---

## 🔍 Example Config (config.yaml)
```yaml
queries:
  incremental_fetch: >
    SELECT id,name,category,price,last_updated
    FROM products
    WHERE last_updated > ?
    ORDER BY last_updated ASC

  last_record: >
    SELECT TOP 1 id,name,category,price,last_updated
    FROM products
    ORDER BY id DESC
```

---

## ✅ Output Example (JSON Line)
```json
{"id": 1, "name": "Laptop", "category": "Electronics", "price": 576.89, "last_updated": "2025-08-03T14:21:33"}
```

---

## 🧠 Authors & Credits
Developed by **Manish Kumar Rai**.

---

## 📄 License
MIT License – Free to use and modify.
