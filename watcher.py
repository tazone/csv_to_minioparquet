import os
import time
import subprocess
import logging
import logging.handlers

# Configure logging
log_file = "watcher.log"
log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=30)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.setLevel(log_level)
logger.addHandler(handler)

# Get environment variables
csv_directory_path = os.getenv('CSV_DIRECTORY_PATH', '/data/csv')

def watch_directory(directory_path):
    logger.info(f"Starting to watch directory: {directory_path}")
    before = dict([(f, None) for f in os.listdir(directory_path)])
    while True:
        time.sleep(10)
        after = dict([(f, None) for f in os.listdir(directory_path)])
        added = [f for f in after if not f in before]
        if added:
            logger.info(f"Detected new files: {', '.join(added)}")
            for file in added:
                if file.endswith('.csv'):
                    logger.info(f"Processing new CSV file: {file}")
                    subprocess.run(["python", "app.py"])
        before = after

if __name__ == "__main__":
    watch_directory(csv_directory_path)
