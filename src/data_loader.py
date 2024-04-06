import os
import pandas as pd
from hdfs import InsecureClient
from pymongo import MongoClient
from pyarrow import parquet as pq, fs

class DataLoader:
    def __init__(self, hdfs_host, hdfs_user, mongo_connection_string, mongo_db_name, mongo_collection_name, logger):
        self.hdfs_client = InsecureClient(f'http://{hdfs_host}', user=hdfs_user)
        self.mongo_client = MongoClient(mongo_connection_string)
        self.mongo_db = self.mongo_client[mongo_db_name]
        self.mongo_collection = self.mongo_db[mongo_collection_name]
        self.logger = logger

    def process_csv_files(self, temporal_landing_csv_dir):
        try:
            csv_files = self.hdfs_client.list(temporal_landing_csv_dir)
            for csv_file in csv_files:
                local_path = os.path.join('/tmp', csv_file)
                hdfs_path = os.path.join(temporal_landing_csv_dir, csv_file)
                
                self.hdfs_client.download(hdfs_path, local_path, overwrite=True)
                
                df = pd.read_csv(local_path)

                parquet_hdfs_path = hdfs_path.replace('.csv', '.parquet')
                with fs.HadoopFileSystem(host=self.hdfs_client.url.split('//')[-1], port=int(self.hdfs_client.root.split(':')[-1])) as hdfs_fs:
                    pq.write_table(pq.Table.from_pandas(df), parquet_hdfs_path, filesystem=hdfs_fs)
                
                self.logger.info(f"CSV file {csv_file} processed and stored as Parquet in HDFS.")
        except Exception as e:
            self.logger.exception(f"Error processing CSV files: {e}")

    def process_json_files(self, temporal_landing_json_dir):
        try:
            json_files = self.hdfs_client.list(temporal_landing_json_dir)
            for json_file in json_files:
                local_path = os.path.join('/tmp', json_file)
                hdfs_path = os.path.join(temporal_landing_json_dir, json_file)

                self.hdfs_client.download(hdfs_path, local_path, overwrite=True)

                with open(local_path, 'r') as f:
                    data = pd.read_json(f)
                    self.mongo_collection.insert_many(data.to_dict('records'))
                
                self.logger.info(f"JSON file {json_file} processed and loaded into MongoDB.")
        except Exception as e:
            self.logger.exception(f"Error processing JSON files: {e}")

    def process_and_load_data(self, temporal_landing_csv_dir, temporal_landing_json_dir):
        """
        Orchestrates the processing and loading of data from TLZ.
        CSV data will be converted to Parquet and stored in HDFS.
        JSON data will be loaded into MongoDB.
        """
        try:
            self.logger.info("Starting CSV files processing and loading into HDFS as Parquet.")
            self.process_csv_files(temporal_landing_csv_dir)
            
            self.logger.info("Starting JSON files processing and loading into MongoDB.")
            self.process_json_files(temporal_landing_json_dir)
            
            self.logger.info("Data processing and loading completed successfully.")
        except Exception as e:
            self.logger.exception("An error occurred during data processing and loading: ", exc_info=e)