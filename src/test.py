from minio import Minio

client = Minio(
    "localhost:9004",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

# Try listing buckets (or create if not exists)
buckets = client.list_buckets()
for bucket in buckets:
    print("✅ Bucket:", bucket.name)
