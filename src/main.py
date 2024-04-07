import os
from dotenv import load_dotenv
import logging.handlers
from data_collector import DataCollector
from data_loader import DataLoader
import argparse

# Create logger object
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Create file handler which logs debug messages
log_file = os.path.join('logs', 'main.log')
log_dir = os.path.dirname(log_file)

if not os.path.exists(log_dir):
    os.makedirs(log_dir)

file_handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=1024 * 1024, backupCount=5)
file_handler.setLevel(logging.DEBUG)

# Create console handler which logs info messages
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

# Create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Load environment variables
load_dotenv()
TEMPORAL_LANDING_DIR_PATH = os.getenv('TEMPORAL_LANDING_DIR_PATH')
PERSISTENT_LANDING_DIR_PATH = os.getenv('PERSISTENT_LANDING_DIR_PATH')
HDFS_HOST = os.getenv('HDFS_HOST')
HDFS_PORT = int(os.getenv('HDFS_PORT'))
HDFS_USER = os.getenv('HDFS_USER')
MONGO_DB_URL = os.getenv('MONGO_DB_URL')
MONGO_DB_PORT = int(os.getenv('MONGO_DB_PORT'))
MONGO_DB_NAME = os.getenv('MONGO_DB_NAME')
MONGO_COLLECTION_NAME = os.getenv('MONGO_COLLECTION_NAME')

def main():
    # Create argument parser
    parser = get_parser()
    args = parser.parse_args()
    process = args.process

    if process == 'data_collector':
        try:
            data_collector = DataCollector(
                TEMPORAL_LANDING_DIR_PATH,
                HDFS_HOST,
                HDFS_PORT,
                HDFS_USER,
                logger)
            
            # data_collector.delete_hdfs_directory(TEMPORAL_LANDING_DIR_PATH)
            # data_collector.delete_hdfs_directory(PERSISTENT_LANDING_DIR_PATH)
            # data_collector.delete_unwanted_hdfs_directories()

            # Collect local files
            data_collector.collect_local_files_to_hdfs()
            # Collect external files
            data_collector.collect_data_from_opendata('accidents-gu-bcn')
            logger.info('Data collection completed successfully.')
        except Exception as e:
            logger.exception(f'Error occurred during data collection: {e}')
    elif(process == 'data_loader'):
        try:
            data_loader = DataLoader(
                PERSISTENT_LANDING_DIR_PATH,
                HDFS_HOST,
                HDFS_PORT,
                HDFS_USER,
                MONGO_DB_NAME,
                MONGO_COLLECTION_NAME,
                logger,
                mongo_db_url=MONGO_DB_URL,
                mongo_db_port=MONGO_DB_PORT)

            data_loader.process_and_load_data(TEMPORAL_LANDING_DIR_PATH)
        except Exception as e:
            logger.exception(f'Error occurred during data loading: {e}')
    
def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("process", choices=['data_collector', 'data_loader'],
                        help="Data Governance Process",
                        type=str)
    return parser


if __name__ == '__main__':
    main()