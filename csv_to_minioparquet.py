import os
import hashlib
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from minio import Minio
from datetime import datetime
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Initialize logging
log_file = "data_processing.log"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Initialize MinIO client
minio_client = Minio(
    "minio-server:9000",
    access_key="your-access-key",
    secret_key="your-secret-key",
    secure=False
)

# Path to the directory containing CSV files
csv_directory_path = "path/to/csv/files"

# Path to the checkpoint file
checkpoint_file = "upload_checkpoint.txt"

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
    logging.info("Processing chunk to Parquet Table.")
    return pa.Table.from_pandas(chunk)

def read_csv_in_chunks(file_path, chunksize=10000):
    # Read CSV file in chunks
    logging.info(f"Reading CSV file in chunks: {file_path}")
    chunks = pd.read_csv(file_path, chunksize=chunksize)
    table_list = []
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_csv_chunk, chunk) for chunk in chunks]
        for future in as_completed(futures):
            table_list.append(future.result())
    logging.info(f"Finished reading CSV file in chunks: {file_path}")
    return pa.concat_tables(table_list)

def csv_to_parquet_and_upload(csv_file_path, bucket_name, object_name, max_retries=3, retry_delay=5):
    attempt = 0
    while attempt < max_retries:
        try:
            # Calculate MD5 for CSV file
            csv_md5 = calculate_md5(csv_file_path)
            logging.info(f"Calculated MD5 for CSV file {csv_file_path}: {csv_md5}")

            # Read CSV file in chunks and convert to Parquet
            logging.info(f"Starting conversion of CSV to Parquet: {csv_file_path}")
            table = read_csv_in_chunks(csv_file_path)
            parquet_file_path = csv_file_path.replace(".csv", ".parquet")
            pq.write_table(table, parquet_file_path)
            logging.info(f"Converted to Parquet file: {parquet_file_path}")

            # Calculate local MD5 for Parquet file
            local_md5 = calculate_md5(parquet_file_path)
            logging.info(f"Calculated local MD5 for Parquet file: {local_md5}")

            # Upload Parquet file to MinIO
            logging.info(f"Uploading Parquet file to MinIO: {parquet_file_path}")
            result = minio_client.fput_object(
                bucket_name=bucket_name,
                object_name=object_name,
                file_path=parquet_file_path
            )
            logging.info(f"File {object_name} successfully uploaded to {bucket_name}.")

            # Compare MD5
            etag = result.etag
            logging.info(f"ETag from MinIO: {etag}")

            if local_md5 == etag:
                logging.info(f"MD5 checksum match for {object_name}.")
                # Remove local parquet file after upload
                os.remove(parquet_file_path)
                logging.info(f"Local Parquet file removed: {parquet_file_path}")
                # Update the checkpoint file
                with open(checkpoint_file, 'a') as f:
                    f.write(f"{csv_file_path}:{csv_md5}\n")
                return True
            else:
                logging.error(f"MD5 checksum mismatch for {object_name}.")
                return False

        except Exception as e:
            attempt += 1
            logging.error(f"Error during processing {csv_file_path}: {e}. Attempt {attempt} of {max_retries}.")
            if attempt < max_retries:
                logging.info(f"Retrying upload for {csv_file_path} after {retry_delay} seconds.")
                time.sleep(retry_delay)

    logging.error(f"Failed to upload {csv_file_path} after {max_retries} attempts.")
    return False

def upload_data_with_confirmation(csv_file_path, bucket_name, object_name):
    # Calculate MD5 for CSV file
    csv_md5 = calculate_md5(csv_file_path)
    checkpoint_entry = f"{csv_file_path}:{csv_md5}"
    # Check if the current file has already been uploaded
    logging.info(f"Checking if file {checkpoint_entry} has already been uploaded.")
    if checkpoint_entry not in uploaded_files:
        logging.info(f"File {checkpoint_entry} not found in checkpoint. Starting upload process.")
        success = csv_to_parquet_and_upload(csv_file_path, bucket_name, object_name)
        if success:
            uploaded_files.add(checkpoint_entry)
            logging.info(f"File {object_name} uploaded and checkpoint updated.")
            return True
        else:
            logging.error(f"Failed to upload {object_name}.")
            return False
    else:
        logging.info(f"File {checkpoint_entry} has already been uploaded. Skipping.")
        return False

def process_files_in_directory(directory_path, bucket_name):
    # Get list of all CSV files in the directory, sorted by creation time
    logging.info(f"Scanning directory for CSV files: {directory_path}")
    csv_files = [f for f in os.listdir(directory_path) if f.endswith('.csv')]
    csv_files.sort(key=lambda x: os.path.getctime(os.path.join(directory_path, x)))

    with ThreadPoolExecutor() as executor:
        futures = []
        for csv_file in csv_files:
            csv_file_path = os.path.join(directory_path, csv_file)
            processing_date = datetime.now().strftime('%Y-%m-%d')
            object_name = f"{processing_date}/{csv_file.replace('.csv', '.parquet')}"
            logging.info(f"Scheduling upload for {csv_file_path} as {object_name}")
            futures.append(executor.submit(upload_data_with_confirmation, csv_file_path, bucket_name, object_name))
        for future in as_completed(futures):
            future.result()
    logging.info("Finished processing all files in directory.")

# Ensure bucket exists
bucket_name = "your-bucket"
if not minio_client.bucket_exists(bucket_name):
    logging.info(f"Bucket {bucket_name} does not exist. Creating bucket.")
    minio_client.make_bucket(bucket_name)

# Process files in the directory
logging.info(f"Starting to process files in directory: {csv_directory_path}")
process_files_in_directory(csv_directory_path, bucket_name)
logging.info("Finished processing files in directory.")
