import os
import io
import pandas as pd
from hdfs import InsecureClient
from pymongo import MongoClient
from pyarrow import parquet as pq, fs
from pyarrow import csv as pc
import json
import paramiko

class DataLoader:
    def __init__(self, persistent_landing_dir, hdfs_host, hdfs_port, hdfs_user, mongo_db_name, mongo_collection_name, logger, mongo_db_url = 'localhost', mongo_db_port = 27017):
        self.hdfs_host = hdfs_host
        self.hdfs_port = hdfs_port
        self.hdfs_user = hdfs_user
        self.persistent_landing_dir=persistent_landing_dir
        self.logger = logger
        self.client = InsecureClient(f'http://{self.hdfs_host}:{self.hdfs_port}', user=self.hdfs_user)
        self.create_hdfs_dir(self.persistent_landing_dir)
        self.mongo_client = MongoClient(mongo_db_url, mongo_db_port)
        self.mongo_db = self.mongo_client[mongo_db_name]
        self.mongo_collection = self.mongo_db[mongo_collection_name]
        self.metadata_collection = self.mongo_db['metadata']

    def create_hdfs_dir(self, folder):
        """
            Creates a directory in HDFS if it does not already exist.
            Args:
                folder (str): The name of the directory to create.
        """
        try:
            if self.client.status(folder, strict=False) is None:
                self.client.makedirs(folder)
                self.logger.info(f"Directory {folder} created successfully.")
            else:
                self.logger.info(f"Directory {folder} already exists.")
        except Exception as e:
            self.logger.exception(e)

    def file_already_processed(self, filename, persistent_dir,  file_type):
        """
        Checks if the file has already been processed.
        """
        if file_type == 'csv':
            parquet_path = f"{persistent_dir}/{filename.replace('.csv', '.parquet')}"
            return self.client.status(parquet_path, strict=False) is not None
        elif file_type == 'json':
            return self.metadata_collection.find_one({"filename": filename}) is not None
        else:
            return False
    
    def mark_file_as_processed(self, filename, file_type):
        """
        Marks the file as processed in the appropriate metadata storage.
        """
        if file_type == 'json':
            self.metadata_collection.insert_one({"filename": filename})

    def process_csv_files(self, temporal_landing_csv_dir, persistent_landing_csv_dir):
        try:
            csv_files = self.client.list(temporal_landing_csv_dir)
            for csv_file in csv_files:
                if not csv_file.endswith('.csv') or self.file_already_processed(csv_file, persistent_landing_csv_dir, 'csv'):
                    continue  # Skip non-CSV files
                with self.client.read(f'{temporal_landing_csv_dir}/{csv_file}') as reader:
                    file_content = reader.read()
                file_io = io.BytesIO(file_content)
                table = pc.read_csv(file_io)

                hdfs_csv_path = f"{persistent_landing_csv_dir}/{csv_file}"
                # print("Table: ", table)
                parquet_hdfs_path = hdfs_csv_path.replace('.csv', '.parquet')
                
                buffer = io.BytesIO()

                pq.write_table(table, buffer)
                buffer.seek(0)
                data_bytes = buffer.read()

                with self.client.write(parquet_hdfs_path, overwrite=True) as writer:
                    writer.write(data_bytes)
                self.logger.info(f"File '{csv_file}' uploaded to HDFS directory: {persistent_landing_csv_dir}")
                    
                #with fs.HadoopFileSystem(host=self.client.url.split('//')[-1], port=int(self.client.root.split(':')[-1])) as hdfs_fs:
                # with fs.HadoopFileSystem(host=self.hdfs_host, port=self.hdfs_port) as hdfs_fs:
                #     pq.write_table(pq.Table.from_pandas(df), parquet_hdfs_path, filesystem=hdfs_fs)
                # with open(parquet_hdfs_path, 'rb') as file:
                #     parquet_data = file.read()
                #     self.upload_file_to_hdfs(os.path.basename(parquet_hdfs_path), parquet_data, os.path.dirname(parquet_hdfs_path))
                
                self.logger.info(f"CSV file {csv_file} processed and stored as Parquet in HDFS.")
        except Exception as e:
            self.logger.exception(f"Error processing CSV files: {e}")

    def process_json_files(self, temporal_landing_json_dir, persistent_landing_json_dir):
        try:
            json_files = self.client.list(temporal_landing_json_dir)
            for json_file in json_files:
                if not json_file.endswith('.json') or self.file_already_processed(json_file, persistent_landing_json_dir, 'json'):
                    continue
                with self.client.read(f"{temporal_landing_json_dir}/{json_file}") as reader:
                    json_content = reader.read()
                    documents = json.loads(json_content)
                    if not isinstance(documents, list) or not documents:
                        self.logger.error(f"Error with file {json_file}.")
                        continue
                    self.mongo_collection.insert_many(documents)
                    self.mark_file_as_processed(json_file, 'json')
                # hdfs_path = os.path.join(temporal_landing_json_dir, json_file)
                
                self.logger.info(f"JSON file {json_file} processed and loaded into MongoDB.")
        except Exception as e:
            self.logger.exception(f"Error processing JSON files: {e}")

    def process_and_load_data(self, temporal_landing_dir):
        """
        Orchestrates the processing and loading of data from TLZ.
        CSV data will be converted to Parquet and stored in HDFS.
        JSON data will be loaded into MongoDB.
        """
        try:
            self.logger.info("Starting CSV files processing and loading into HDFS as Parquet.")
            self.process_csv_files(temporal_landing_dir+ "/opendatabcn_income_csv", self.persistent_landing_dir+"/opendatabcn_income_parquet")
            self.process_csv_files(temporal_landing_dir+ "/lookup_csv", self.persistent_landing_dir+"/lookup_parquet")
            
            vm = paramiko.SSHClient()
            vm.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            vm.connect('10.4.41.49', username='bdm', password='bdm2024')
            command = '/home/bdm/BDM_Software/mongodb/bin/mongod --bind_ip_all --dbpath /home/bdm/BDM_Software/data/mongodb_data/'
            stdin,stdout,stderr=vm.exec_command(command)
            print(stdout.readlines())
            print(stderr.readlines())
            self.logger.info("Starting JSON files processing and loading into MongoDB.")
            self.process_json_files(temporal_landing_dir+"/idealista_json", self.persistent_landing_dir+"/idealista_json")
            
            # self.logger.info("Data processing and loading completed successfully.")
            # for collection in self.mongo_db.list_collection_names():
            #     print(collection)
            #     print(self.mongo_db[collection].count_documents({}))
        except Exception as e:
            self.logger.exception("An error occurred during data processing and loading: ", exc_info=e)