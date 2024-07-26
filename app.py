import os
import hashlib
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from minio import Minio
from datetime import datetime
import logging
import logging.handlers
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
log_file = "data_processing.log"
log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=30)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.setLevel(log_level)
logger.addHandler(handler)

# Get environment variables
csv_directory_path = os.getenv('CSV_DIRECTORY_PATH', '/data/csv')
checkpoint_file = os.getenv('CHECKPOINT_FILE', '/data/checkpoint.txt')
minio_server = os.getenv('MINIO_SERVER')
minio_access_key = os.getenv('MINIO_ACCESS_KEY')
minio_secret_key = os.getenv('MINIO_SECRET_KEY')
bucket_name = os.getenv('MINIO_BUCKET')

# Initialize MinIO client
minio_client = Minio(
    minio_server,
    access_key=minio_access_key,
    secret_key=minio_secret_key,
    secure=False
)

# Ensure the checkpoint file exists
if not os.path.exists(checkpoint_file):
    open(checkpoint_file, 'w').close()

# Read the checkpoint file to get the list of uploaded files
def read_checkpoint():
    if not os.path.exists(checkpoint_file):
        return set()
    with open(checkpoint_file, 'r') as f:
        return set(f.read().splitlines())

uploaded_files = read_checkpoint()

def calculate_md5(file_path):
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def process_csv_chunk(chunk):
    # Convert chunk of DataFrame to Parquet Table
    logger.info("Processing chunk to Parquet Table.")
    return pa.Table.from_pandas(chunk)

def read_csv_in_chunks(file_path, chunksize=10000):
    # Read CSV file in chunks
    logger.info(f"Reading CSV file in chunks: {file_path}")
    chunks = pd.read_csv(file_path, chunksize=chunksize)
    table_list = []
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_csv_chunk, chunk) for chunk in chunks]
        for future in as_completed(futures):
            table_list.append(future.result())
    logger.info(f"Finished reading CSV file in chunks: {file_path}")
    return pa.concat_tables(table_list)

def csv_to_parquet_and_upload(csv_file_path, bucket_name, object_name, max_retries=3, retry_delay=5):
    attempt = 0
    while attempt < max_retries:
        try:
            # Calculate MD5 for CSV file
            csv_md5 = calculate_md5(csv_file_path)
            logger.info(f"Calculated MD5 for CSV file {csv_file_path}: {csv_md5}")

            # Read CSV file in chunks and convert to Parquet
            logger.info(f"Starting conversion of CSV to Parquet: {csv_file_path}")
            table = read_csv_in_chunks(csv_file_path)
            parquet_file_path = csv_file_path.replace(".csv", ".parquet")
            pq.write_table(table, parquet_file_path)
            logger.info(f"Converted to Parquet file: {parquet_file_path}")

            # Calculate local MD5 for Parquet file
            local_md5 = calculate_md5(parquet_file_path)
            logger.info(f"Calculated local MD5 for Parquet file: {local_md5}")

            # Upload Parquet file to MinIO
            logger.info(f"Uploading Parquet file to MinIO: {parquet_file_path}")
            result = minio_client.fput_object(
                bucket_name=bucket_name,
                object_name=object_name,
                file_path=parquet_file_path
            )
            logger.info(f"File {object_name} successfully uploaded to {bucket_name}.")

            # Compare MD5
            etag = result.etag
            logger.info(f"ETag from MinIO: {etag}")

            if local_md5 == etag:
                logger.info(f"MD5 checksum match for {object_name}.")
                # Remove local parquet file after upload
                os.remove(parquet_file_path)
                logger.info(f"Local Parquet file removed: {parquet_file_path}")
                # Update the checkpoint file
                with open(checkpoint_file, 'a') as f:
                    f.write(f"{csv_file_path}:{csv_md5}\n")
                return True
            else:
                logger.error(f"MD5 checksum mismatch for {object_name}.")
                return False

        except Exception as e:
            attempt += 1
            logger.error(f"Error during processing {csv_file_path}: {e}. Attempt {attempt} of {max_retries}.")
            if attempt < max_retries:
                logger.info(f"Retrying upload for {csv_file_path} after {retry_delay} seconds.")
                time.sleep(retry_delay)

    logger.error(f"Failed to upload {csv_file_path} after {max_retries} attempts.")
    return False

def upload_data_with_confirmation(csv_file_path, bucket_name, object_name):
    # Calculate MD5 for CSV file
    csv_md5 = calculate_md5(csv_file_path)
    checkpoint_entry = f"{csv_file_path}:{csv_md5}"
    # Check if the current file has already been uploaded
    logger.info(f"Checking if file {checkpoint_entry} has already been uploaded.")
    if checkpoint_entry not in uploaded_files:
        logger.info(f"File {checkpoint_entry} not found in checkpoint. Starting upload process.")
        success = csv_to_parquet_and_upload(csv_file_path, bucket_name, object_name)
        if success:
            uploaded_files.add(checkpoint_entry)
            logger.info(f"File {object_name} uploaded and checkpoint updated.")
            return True
        else:
            logger.error(f"Failed to upload {object_name}.")
            return False
    else:
        logger.info(f"File {checkpoint_entry} has already been uploaded. Skipping.")
        return False

def process_files_in_directory(directory_path, bucket_name):
    # Get list of all CSV files in the directory, sorted by creation time
    logger.info(f"Scanning directory for CSV files: {directory_path}")
    csv_files = [f for f in os.listdir(directory_path) if f.endswith('.csv')]
    csv_files.sort(key=lambda x: os.path.getctime(os.path.join(directory_path, x)))

    with ThreadPoolExecutor() as executor:
        futures = []
        for csv_file in csv_files:
            csv_file_path = os.path.join(directory_path, csv_file)
            processing_date = datetime.now().strftime('%Y-%m-%d')
            object_name = f"{processing_date}/{csv_file.replace('.csv', '.parquet')}"
            logger.info(f"Scheduling upload for {csv_file_path} as {object_name}")
            futures.append(executor.submit(upload_data_with_confirmation, csv_file_path, bucket_name, object_name))
        for future in as_completed(futures):
            future.result()
    logger.info("Finished processing all files in directory.")

# Ensure bucket exists
if not minio_client.bucket_exists(bucket_name):
    logger.info(f"Bucket {bucket_name} does not exist. Creating bucket.")
    minio_client.make_bucket(bucket_name)

# Process files in the directory
logger.info(f"Starting to process files in directory: {csv_directory_path}")
process_files_in_directory(csv_directory_path, bucket_name)
logger.info("Finished processing files in directory.")
