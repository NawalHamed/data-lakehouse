from minio import Minio
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Minio client
client = Minio(
    "localhost:9004",  # Use the custom S3 API port (9004)
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

minio_bucket = "iceberg-bucket"

# Check if the bucket exists and create it if not
found = client.bucket_exists(minio_bucket)
if not found:
    client.make_bucket(minio_bucket)

# Define file paths for source files
source_file_1 = './data/data.csv'     # First CSV file
source_file_2 = './data/data2.csv'    # Second CSV file
source_file_3 = './data/data3.json'   # JSON file

# Define destination file names
destination_file_1 = 'data.csv'
destination_file_2 = 'data2.csv'
destination_file_3 = 'data3.json'

# Upload files to Minio bucket
client.fput_object(minio_bucket, destination_file_1, source_file_1)
client.fput_object(minio_bucket, destination_file_2, source_file_2)
client.fput_object(minio_bucket, destination_file_3, source_file_3)

# Create SparkSession for Iceberg with Minio configuration
iceberg_builder = SparkSession.builder \
    .appName("iceberg-concurrent-write-isolation-test") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.iceberg:iceberg-hive-runtime:1.5.0") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
    .config("spark.sql.catalog.spark_catalog.warehouse", f"s3a://{minio_bucket}/iceberg_data/") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9004") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .enableHiveSupport()

# Build the SparkSession for Iceberg
iceberg_spark = iceberg_builder.getOrCreate()

# Iceberg table location in Minio
iceberg_table_location = f"s3a://{minio_bucket}/iceberg_data/default"

# Read the current row count in the Iceberg table
iceberg_df_before = iceberg_spark.read.format("iceberg").load(f"{iceberg_table_location}/iceberg_table_name")
count_before = iceberg_df_before.count()

print(f"Row count before appending data: {count_before}")

# Define the schema to match the Iceberg table structure
schema = StructType([
    StructField("year", IntegerType(), True),
    StructField("age", IntegerType(), True),
    StructField("school", IntegerType(), True),
    StructField("group", IntegerType(), True),
    StructField("topic", IntegerType(), True),
    StructField("count", StringType(), True)
])

# Load both CSV files into Spark DataFrames with the defined schema
df1 = iceberg_spark.read.format('csv').option('header', 'true').option('inferSchema', 'false').schema(schema).load(source_file_1)
df2 = iceberg_spark.read.format('csv').option('header', 'true').option('inferSchema', 'false').schema(schema).load(source_file_2)

# Load JSON file into Spark DataFrame with the defined schema
df3 = iceberg_spark.read.format('json').schema(schema).load(source_file_3)

# Using a loop to append DataFrames to the Iceberg table and ensuring transactional isolation
def append_to_iceberg(df):
    try:
        df.write \
            .format("iceberg") \
            .mode("append") \
            .saveAsTable("iceberg_table_name")
        print(f"Data successfully appended from {df}")
    except Exception as e:
        print(f"Error while appending data: {e}")

# Append all DataFrames to the Iceberg table
for df in [df1, df2, df3]:
    append_to_iceberg(df)

# Read the row count in the Iceberg table after appending
iceberg_df_after = iceberg_spark.read.format("iceberg").load(f"{iceberg_table_location}/iceberg_table_name")
count_after = iceberg_df_after.count()

print(f"Row count after appending data: {count_after}")

# Compare the counts to see if the data was appended
if count_after > count_before:
    print(f"✅ Data was successfully appended! Rows added: {count_after - count_before}")
else:
    print("⚠️ No data was appended.")

# --- Verification Option 1: Count rows grouped by year ---
print("\n📊 Row count grouped by year:")
iceberg_spark.sql("""
    SELECT year, COUNT(*) as count
    FROM iceberg_table_name
    GROUP BY year
    ORDER BY year
""").show()

# --- Verification Option 2: Show all rows with year = 2024 ---
print("\n🔍 Records where year = 2024:")
iceberg_spark.sql("""
    SELECT *
    FROM iceberg_table_name
    WHERE year = 2024
""").show()

# --- Verification Option 3: Preview the uploaded JSON file content before writing ---
print("\n📦 Preview of data3.json content before writing to Iceberg:")
df3.show()

# --- Additional Verification: Querying Parquet Files ---
parquet_file_path = 's3a://your-bucket-name/path/to/parquet/files/'

# Read Parquet files into DataFrame
parquet_df = iceberg_spark.read.parquet(parquet_file_path)

# Register the DataFrame as a temporary SQL table
parquet_df.createOrReplaceTempView("parquet_table")

# Querying the Parquet data
print("\n📊 Querying Parquet Files - Count by Year:")
iceberg_spark.sql("""
    SELECT year, COUNT(*) as count
    FROM parquet_table
    GROUP BY year
    ORDER BY year
""").show()
