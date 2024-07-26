# Use an official Python runtime as a parent image
FROM python:3.9

# Set the working directory
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
RUN apt-get update && apt-get install -y inotify-tools

# Make port 80 available to the world outside this container
EXPOSE 80

# Define environment variables
ENV CSV_DIRECTORY_PATH /data/csv
ENV CHECKPOINT_FILE /data/checkpoint.txt
ENV MINIO_SERVER minio-server:9000
ENV MINIO_ACCESS_KEY your-access-key
ENV MINIO_SECRET_KEY your-secret-key
ENV MINIO_BUCKET your-bucket
ENV LOG_LEVEL INFO

# Run watcher.py when the container launches
CMD ["python", "watcher.py"]
