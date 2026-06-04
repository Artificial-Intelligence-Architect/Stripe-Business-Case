# Deployment Guide - Stripe Data Architecture

This guide provides **step-by-step instructions** to deploy the Stripe data architecture locally or in the cloud. It covers all components: **OLTP (PostgreSQL + Citus), OLAP (Snowflake), NoSQL (MongoDB Atlas), Streaming (Kafka + Debezium), Orchestration (Airflow + dbt), and Monitoring (Evidently AI)**.

---

## 📋 Prerequisites

### 1. **Local Environment**
| Tool/Service          | Version       | Purpose                          | Installation Command/Link                          |
|-----------------------|---------------|----------------------------------|------------------------------------------------------|
| Docker                | 20.10+        | Containerisation                 | [Install Docker](https://docs.docker.com/get-docker/) |
| Docker Compose        | 2.20+         | Multi-container orchestration    | Included with Docker Desktop                         |
| Python                | 3.10+         | Scripting, Airflow, dbt           | [Install Python](https://www.python.org/downloads/) |
| Git                   | Latest        | Version control                  | [Install Git](https://git-scm.com/downloads)        |
| `pip`                 | Latest        | Python package manager           | `python -m ensurepip --upgrade`                      |

### 2. **Cloud Services**
| Service               | Provider      | Purpose                          | Sign-Up Link                                      |
|-----------------------|---------------|----------------------------------|------------------------------------------------------|
| MongoDB Atlas         | MongoDB       | NoSQL database                   | [Sign Up](https://www.mongodb.com/atlas/database)   |
| Snowflake             | Snowflake     | OLAP data warehouse               | [Sign Up](https://signup.snowflake.com/)            |
| Confluent Cloud       | Confluent     | Managed Kafka (optional)         | [Sign Up](https://www.confluent.io/confluent-cloud/)|

### 3. **Required Accounts & Credentials**
- **GitHub**: Clone the repository.
- **MongoDB Atlas**: Cluster URL, username, password.
- **Snowflake**: Account URL, username, password, warehouse, database.
- **AWS/GCP/Azure** (optional): For hosting Kafka/PostgreSQL if not using local Docker.

---

## 🚀 Step-by-Step Deployment

---

### **Step 1: Clone the Repository**
```bash
# Clone the repo
git clone https://github.com/Artificial-Intelligence-Architect/Stripe-Business-Case.git
cd Stripe-Business-Case

# Create a virtual environment (recommended)
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# OR
.\.venv\Scripts\activate   # Windows

# Install Python dependencies
pip install -r requirements.txt
```

---

### **Step 2: Deploy PostgreSQL + Citus (OLTP)**

#### **Option A: Local Deployment (Docker)**
1. **Start PostgreSQL + Citus** using Docker Compose:
   ```bash
   cd demo/local_setup
   docker-compose -f docker-compose.yml up -d
   ```
   - This spins up:
     - PostgreSQL (primary + 2 replicas)
     - Citus extension (for horizontal sharding)
     - pgAdmin (web UI at `http://localhost:5050`)

2. **Verify the setup**:
   ```bash
   docker exec -it stripe-postgres psql -U stripe -d stripe_oltp
   ```
   - Run a test query:
     ```sql
     SELECT version();
     ```

3. **Load sample data** (optional):
   ```bash
   docker exec -it stripe-postgres psql -U stripe -d stripe_oltp -f /docker-entrypoint-initdb.d/sample_data.sql
   ```

#### **Option B: Cloud Deployment (AWS RDS/Aurora PostgreSQL)**
1. **Create an RDS instance** with PostgreSQL 15+.
2. **Enable Citus extension** (requires custom parameter group).
3. **Configure security groups** to allow connections from your IP.
4. **Update connection strings** in:
   - `pipeline/debezium/debezium-postgres-connector.json`
   - `sql/oltp/schema.sql` (if referencing cloud endpoints)

---

### **Step 3: Deploy Kafka + Debezium (Streaming)**

#### **Option A: Local Deployment (Docker)**
1. **Start Kafka + Zookeeper + Debezium** using Docker Compose:
   ```bash
   cd demo/local_setup
   docker-compose -f docker-compose-kafka.yml up -d
   ```
   - This spins up:
     - Zookeeper (1 node)
     - Kafka (1 broker)
     - Debezium Connect (with PostgreSQL connector)
     - Kafka UI (web UI at `http://localhost:8080`)

2. **Register the PostgreSQL connector** for CDC:
   ```bash
   curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" \
     -d @../../pipeline/debezium/debezium-postgres-connector.json
   ```

3. **Verify the connector**:
   ```bash
   curl http://localhost:8083/connectors/stripe-postgres-connector/status
   ```

#### **Option B: Cloud Deployment (Confluent Cloud)**
1. **Create a Kafka cluster** in Confluent Cloud.
2. **Set up a PostgreSQL connector** using the Confluent UI:
   - Use the `debezium-postgres-connector.json` file as a template.
   - Update the `database.hostname`, `database.user`, and `database.password` fields.
3. **Test the connector** by producing/consume messages:
   ```bash
   # Consume messages from the pg.transactions topic
   docker exec -it stripe-kafka kafka-console-consumer --bootstrap-server localhost:9092 \
     --topic pg.transactions --from-beginning
   ```

---

### **Step 4: Deploy MongoDB Atlas (NoSQL)**

1. **Create a free cluster** in [MongoDB Atlas](https://www.mongodb.com/atlas/database):
   - Choose **M0 tier** (free) for testing.
   - Select a region close to your deployment (e.g., `eu-west-1` for Europe).

2. **Configure network access**:
   - Add your IP address to the **Network Access** list.
   - Enable **SRV connection string** (recommended).

3. **Create databases and collections**:
   - Use the `nosql/mongodb/mongodb_schema.py` script to create collections and indexes:
     ```bash
     python nosql/mongodb/mongodb_schema.py
     ```
   - This script:
     - Creates `fraud_events`, `user_sessions`, and `app_logs` collections.
     - Sets up indexes (TTL, compound, etc.).
     - Loads sample data from `nosql/mongodb/sample_documents.json`.

4. **Verify the setup**:
   ```bash
   # Connect to MongoDB Atlas
   mongosh "mongodb+srv://<username>:<password>@cluster0.example.mongodb.net/stripe_nosql"
   
   # List collections
   use stripe_nosql
   show collections
   ```

---

### **Step 5: Deploy Snowflake (OLAP)**

1. **Sign up for Snowflake** at [https://signup.snowflake.com/](https://signup.snowflake.com/).
2. **Create a workspace**:
   - Select **Standard Edition** (free trial available).
   - Choose a region (e.g., `eu-west-1`).

3. **Set up the database and schemas**:
   - Run the SQL scripts in `sql/olap/schema.sql` using the Snowflake web UI or `snowsql` CLI:
     ```bash
     # Install snowsql (Snowflake CLI)
     pip install snowflake-connector-python snowflake-snowsql
     
     # Connect to Snowflake
     snowsql -a <account_identifier> -u <username> -p <password>
     ```
   - Execute:
     ```sql
     -- Create database
     CREATE DATABASE stripe_olap;
     
     -- Create schemas
     CREATE SCHEMA stripe_olap.staging;
     CREATE SCHEMA stripe_olap.intermediate;
     CREATE SCHEMA stripe_olap.marts;
     
     -- Run the full schema script
     !source sql/olap/schema.sql
     ```

4. **Configure Snowpipe** (for Kafka → S3 → Snowflake):
   - Set up an **S3 bucket** (AWS) or **GCS bucket** (GCP) for staging data.
   - Create a **Snowpipe** using the script in `pipeline/snowflake/snowpipe_setup.sql`.

---

### **Step 6: Deploy Airflow + dbt (Orchestration)**

1. **Start Airflow** using Docker Compose:
   ```bash
   cd demo/local_setup
   docker-compose -f docker-compose-airflow.yml up -d
   ```
   - This spins up:
     - Airflow webserver (`http://localhost:8080`)
     - Airflow scheduler
     - PostgreSQL (for Airflow metadata)
     - Redis (for Celery broker)

2. **Set