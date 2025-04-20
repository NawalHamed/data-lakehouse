# Data-Lakehouse

# 🏗️ Data Lakehouse with MinIO, Apache Spark, and Apache Iceberg

This project sets up a minimal **Data Lakehouse** architecture using Docker Compose. It integrates:

- **MinIO**: S3-compatible object storage
- **Apache Spark**: Distributed data processing engine
- **Apache Iceberg**: Table format for huge analytic datasets

---

## 📦 Stack Components

| Service         | Description |
|------------------|-------------|
| **MinIO**         | Object storage that mimics Amazon S3. Used as the data lake storage layer. |
| **Spark Master**  | Apache Spark master node with Iceberg runtime. |
| **Spark Worker**  | Spark worker node that connects to the master for task execution. |

---

🚀 Getting Started
1. Clone the repository

git clone https://github.com/your-username/data-lakehouse-minio-spark.git
cd data-lakehouse-minio-spark

2. Launch the services
docker-compose up -d

3. Access the interfaces

Service	URL	Default Credentials
MinIO Console	http://localhost:9005	minioadmin:minioadmin
Spark Master	http://localhost:8082	N/A
Spark Worker	http://localhost:8083	N/A


🧪 Example Use Case
You can use Spark or PySpark to:

Read/write Iceberg tables using s3a://bucket-name/iceberg-table

Query data using Spark SQL with Iceberg integration

Save structured data directly to MinIO via Spark jobs

Example Spark submit command:
spark-submit \
  --master spark://localhost:7077 \
  --packages org.apache.iceberg:iceberg-spark3-runtime:0.12.0 \
  your_spark_script.py


📁 Volumes

Volume	Purpose
minio_data	Persists object storage
jupyter_data	Reserved for Jupyter usage
📌 Future Enhancements
 Integrate Jupyter Notebook

 Add Hive Metastore for Iceberg catalog

 Use Airflow for orchestration

 Add demo scripts and datasets

