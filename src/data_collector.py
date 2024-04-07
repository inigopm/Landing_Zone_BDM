import os
import requests
import json
import csv
from hdfs import InsecureClient

class DataCollector:
    def __init__(self, temporal_landing_dir, hdfs_host, hdfs_port, hdfs_user, logger):
        self.temporal_landing_dir = temporal_landing_dir
        self.hdfs_host = hdfs_host
        self.hdfs_port = hdfs_port
        self.hdfs_user = hdfs_user
        self.logger = logger
        try:
            self.client = InsecureClient(f'http://{self.hdfs_host}:{self.hdfs_port}', user=self.hdfs_user)
            self.logger.info(f"Connection to HDFS has been established successfully.")
            self.create_hdfs_dir(os.path.join(self.temporal_landing_dir))
        except Exception as e:
            self.client.close()
            self.logger.exception(e)

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
            self.client.close()
            self.logger.exception(e)

    def upload_file_to_hdfs(self, filename, data_bytes, hdfs_dir_path):
        """
        Uploads a file to HDFS.
        Args:
            filename (str): Name of the file to be uploaded.
            data_bytes (bytes): The data bytes of the file.
            hdfs_dir_path (str): Path to the HDFS directory where the file will be uploaded.
        """
        try:            
            hdfs_file_path = hdfs_dir_path +"/"+ filename
            # Write the file to HDFS
            with self.client.write(hdfs_file_path, overwrite=True) as writer:
                writer.write(data_bytes)
            self.logger.info(f"File '{filename}' uploaded to HDFS directory: {hdfs_dir_path}")
        except Exception as e:
            self.logger.exception(f"Failed to upload file '{filename}' to HDFS directory: {hdfs_dir_path}, Error: {e}")

    def collect_local_files_to_hdfs(self):
        """
        Collects local files data and uploads them to HDFS.
        """
        try:
            json_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'idealista')
            csv_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'opendatabcn-income')
            lookup_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'lookup_tables')

            temporal_landing_opendata_dir = self.temporal_landing_dir+ '/opendatabcn_income_csv'
            temporal_landing_idealista_dir =self.temporal_landing_dir+ '/idealista_json'
            temporal_landing_lookup_dir = self.temporal_landing_dir+ '/lookup_csv'

            self.create_hdfs_dir(temporal_landing_opendata_dir)
            self.create_hdfs_dir(temporal_landing_idealista_dir)
            self.create_hdfs_dir(temporal_landing_lookup_dir)

            self.upload_files_in_directory(csv_dir, temporal_landing_opendata_dir, '.csv')
            self.upload_files_in_directory(lookup_dir, temporal_landing_lookup_dir, '.csv')
            self.upload_files_in_directory(json_dir, temporal_landing_idealista_dir, '.json')

        except Exception as e:
            self.logger.exception(e)

    def upload_files_in_directory(self, local_dir, hdfs_dir, extension):
        """
        Uploads files from local directory to HDFS.
        """
        for filename in os.listdir(local_dir):
            if filename.endswith(extension):
                filepath = os.path.join(local_dir, filename)
                with open(filepath, 'rb') as file:
                    data = file.read()
                    self.upload_file_to_hdfs(filename, data, hdfs_dir)

    def collect_data_from_opendata(self, dataset_id):
        """
        Collects data from the CKAN API for the given dataset ID and uploads it to HDFS.
        Args:
            dataset_id (str): Nom del dataset de opendata Barcelona.
        """
        try:
            base_url = "https://opendata-ajuntament.barcelona.cat/data/api/3/"
            endpoint_url = f"{base_url}action/package_show?id={dataset_id}"
            # Set up the headers for the API request
            headers = {'Content-Type': 'application/json'}
            # Send a GET request to the API endpoint
            response = requests.get(endpoint_url, headers=headers)

            if response.status_code == 200:
                temporal_landing_dir = os.path.join(self.temporal_landing_dir, 'opendatabcn_accidents_csv')
                self.create_hdfs_dir(temporal_landing_dir)

                resources = response.json()['result']['resources']
                for resource in resources:
                    resource_url = resource['url']
                    if resource_url:
                        # Download CSV data
                        csv_data = requests.get(resource_url).content
                        # Only upload .csv files
                        if not resource['name'].endswith('.xml'):
                            # Upload data to HDFS                        
                            self.upload_file_to_hdfs(resource['name'], csv_data, temporal_landing_dir)
            else:
                self.logger.error(f"Failed to fetch dataset from CKAN API: {response.status_code}")
        except Exception as e:
            self.logger.exception(f"An error occurred while collecting data from CKAN API: {e}")

    def delete_hdfs_directory(self, hdfs_dir_path):
        """
        Delete a directory and all its contents from HDFS.
        Args:
            hdfs_dir_path (str): Path to the HDFS directory to be deleted.
        """
        try:
            if self.client.content(hdfs_dir_path, strict=False) is not None:
                self.client.delete(hdfs_dir_path, recursive=True)
                self.logger.info(f"Directory '{hdfs_dir_path}' and its contents deleted successfully from HDFS.")
            else:
                self.logger.info(f"Directory '{hdfs_dir_path}' does not exist in HDFS.")
        except Exception as e:
            self.logger.exception(f"Failed to delete directory '{hdfs_dir_path}' from HDFS: {e}")

    # def delete_unwanted_hdfs_directories(self):
    #     """
    #     Delete all directories except 'hbase' and 'user' and their contents from HDFS.
    #     """
    #     root_dir_path = "/"  # Assuming the root path is '/', adjust if it's different
    #     try:
    #         # List all entries in the root directory
    #         entries = self.client.list(root_dir_path, status=True)
    #         print(entries)
    #         for entry in entries:
    #             # Check if the entry is a directory and not in the excluded list
    #             if entry[1]['type'] == 'FILE' and entry[0] not in ['hbase', 'user']:
    #                 directory_path = f"{root_dir_path}{entry[0]}"
    #                 # Delete the directory and its contents
    #                 self.client.delete(directory_path, recursive=True)
    #                 self.logger.info(f"Directory '{directory_path}' and its contents deleted successfully from HDFS.")
    #     except Exception as e:
    #         self.logger.exception(f"Failed to delete directories from HDFS: {e}")
