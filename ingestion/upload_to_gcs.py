from google.cloud import storage
from dotenv import load_dotenv
import os

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME")
DATA_DIR = "data/"

def upload_file(bucket, source_path, destination_blob):
    blob = bucket.blob(destination_blob)
    print(f"Uploading {source_path} → gs://{BUCKET_NAME}/{destination_blob}")
    blob.upload_from_filename(source_path)
    print(f"Done: {destination_blob}")

def main():
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)

    for filename in os.listdir(DATA_DIR):
        if filename.endswith(".csv"):
            source_path = os.path.join(DATA_DIR, filename)
            destination_blob = f"raw/{filename}"
            upload_file(bucket, source_path, destination_blob)

    print("\nAll files uploaded successfully!")

if __name__ == "__main__":
    main()
