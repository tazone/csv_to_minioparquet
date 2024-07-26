# CSV to Parquet Processing and Upload Script

## Overview

This script processes CSV files from a specified directory, converts them to Parquet format, and uploads them to a bucket in MinIO. The script includes dynamic schema detection for CSV files, handles large files by reading in chunks, and utilizes parallel processing to improve performance. It also uses a checkpoint file based on a combination of the file name and its MD5 checksum to prevent uploading duplicates. Additionally, the script includes a retry mechanism for uploads in case of failure and logs all key operations and errors to a log file, enabling monitoring and troubleshooting. The script can run as a Docker container and continuously monitor a directory for new CSV files.

## Features

- **Dynamic Schema Detection**: Automatically detects the schema of CSV files during processing.
- **Large File Handling**: Reads CSV files in chunks to manage large files efficiently.
- **Parallel Processing**: Utilizes `ThreadPoolExecutor` for concurrent processing of CSV chunks and uploads.
- **Duplicate Prevention**: Uses a checkpoint file based on file name and MD5 checksum to track uploaded files.
- **Retry Mechanism**: Retries file upload up to a specified number of times in case of failures.
- **Logging**: Logs all key operations and errors to a log file for monitoring and troubleshooting, with log rotation.
- **Docker Integration**: Runs as a Docker container that monitors a specified directory for new CSV files.
- **Configurable Log Level**: Log level can be set via an environment variable.

## Prerequisites

- Docker
- MinIO server (running and accessible)

## Installation

1. Clone this repository:
    ```sh
    git clone <repository-url>
    cd <repository-directory>
    ```

2. Create a `requirements.txt` file with the following content:
    ```txt
    pandas
    pyarrow
    minio
    ```

3. Create `app.py` and `watcher.py` scripts with the provided content.

## Configuration

Modify the following environment variables in the Docker run command to match your environment:

- `CSV_DIRECTORY_PATH`: Path to the directory containing CSV files (default: `/data/csv`).
- `CHECKPOINT_FILE`: Path to the checkpoint file (default: `/data/checkpoint.txt`).
- `MINIO_SERVER`: MinIO server address (e.g., `minio-server:9000`).
- `MINIO_ACCESS_KEY`: MinIO access key.
- `MINIO_SECRET_KEY`: MinIO secret key.
- `MINIO_BUCKET`: The target bucket in MinIO where files will be uploaded.
- `LOG_LEVEL`: Log level for logging (e.g., `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`).

## Usage

### Building and Running with Docker

1. Build the Docker image:
    ```sh
    docker build -t csv-to-parquet-watcher .
    ```

2. Run the Docker container:
    ```sh
    docker run -d -v /path/to/host/csv:/data/csv \
    -e CSV_DIRECTORY_PATH=/data/csv \
    -e CHECKPOINT_FILE=/data/checkpoint.txt \
    -e MINIO_SERVER=minio-server:9000 \
    -e MINIO_ACCESS_KEY=your-access-key \
    -e MINIO_SECRET_KEY=your-secret-key \
    -e MINIO_BUCKET=your-bucket \
    -e LOG_LEVEL=DEBUG \
    csv-to-parquet-watcher
    ```

Replace `/path/to/host/csv` with the path to your local directory containing CSV files. Ensure to set the appropriate values for your MinIO configuration and desired log level.

### Script Explanation

#### Import Libraries

The script imports necessary libraries including `os`, `hashlib`, `pandas`, `pyarrow`, `minio`, `datetime`, `logging`, `logging.handlers`, `time`, and `concurrent.futures`.

#### Logging Setup

Initializes logging to log all key operations and errors to `data_processing.log` with log rotation (max 30 log files, each up to 5 MB). The log level is set via the `LOG_LEVEL` environment variable.

#### MinIO Client Initialization

Initializes the MinIO client with the server details and credentials.

#### Read Checkpoint File

Defines a function `read_checkpoint` to read the checkpoint file and return a set of uploaded files.

#### Calculate MD5

Defines a function `calculate_md5` to calculate the MD5 checksum of a given file.

#### Process CSV Chunk

Defines a function `process_csv_chunk` to convert a chunk of a CSV DataFrame to a Parquet Table.

#### Read CSV in Chunks

Defines a function `read_csv_in_chunks` to read a CSV file in chunks and convert each chunk to a Parquet Table, using parallel processing.

#### Convert CSV to Parquet and Upload

Defines a function `csv_to_parquet_and_upload` to:
- Calculate the MD5 checksum for the CSV file.
- Read the CSV file in chunks and convert it to Parquet.
- Calculate the MD5 checksum for the Parquet file.
- Upload the Parquet file to MinIO.
- Compare local and server MD5 checksums.
- Update the checkpoint file upon successful upload.

#### Upload Data with Confirmation

Defines a function `upload_data_with_confirmation` to check if a file has already been uploaded by comparing the file name and MD5 checksum with the checkpoint file.

#### Process Files in Directory

Defines a function `process_files_in_directory` to:
- Scan the specified directory for CSV files.
- Sort files by creation time.
- Schedule the upload tasks using `ThreadPoolExecutor`.

#### Ensure Bucket Exists

Checks if the target bucket exists in MinIO and creates it if it doesn't.

#### Watch Directory

Defines a function `watch_directory` to monitor the specified directory for new CSV files and process them as they are detected.

### Main Processing

The main script initializes the necessary configurations and starts the directory watching process.

## Conclusion

This script provides an efficient and reliable way to process and upload CSV files to MinIO, with robust error handling and logging for monitoring and troubleshooting. By following the configuration and usage instructions, you can easily integrate this script into your data processing pipeline.
