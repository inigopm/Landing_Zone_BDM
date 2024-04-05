import os
import requests
import json
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
        Returns:
            None
        """
        try:            
            hdfs_file_path = os.path.join(hdfs_dir_path, filename)
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
            json_dir = '/data/idealista/'
            csv_dir = '/data/opendatabcn-income/'
            temporal_landing_csv_dir = os.path.join(self.temporal_landing_dir, 'temporal_landing_CSV')
            temporal_landing_json_dir = os.path.join(self.temporal_landing_dir, 'temporal_landing_JSON')

            self.create_hdfs_dir(temporal_landing_csv_dir)
            self.create_hdfs_dir(temporal_landing_json_dir)

            self.upload_files_in_directory(csv_dir, temporal_landing_csv_dir, '.csv')
            self.upload_files_in_directory(json_dir, temporal_landing_json_dir, '.json')

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
            dataset_id (str): The ID of the dataset on CKAN.
        Returns:
            None
        """
        try:
            base_url = "https://opendata-ajuntament.barcelona.cat/data/api/3/"
            endpoint_url = f"{base_url}action/package_show?id={dataset_id}"
            # Set up the headers for the API request
            headers = {'Content-Type': 'application/json'}
            # Send a GET request to the API endpoint
            response = requests.get(endpoint_url, headers=headers)

            if response.status_code == 200:
                temporal_landing_URL_dir = os.path.join(self.temporal_landing_dir, 'temporal_landing_URL')
                self.create_hdfs_dir(temporal_landing_URL_dir)

                resources = response.json()['result']['resources']
                for resource in resources:
                    resource_url = resource['url']
                    if resource_url:
                        self.download_and_upload(resource_url, temporal_landing_URL_dir)

            else:
                self.logger.error(f"Failed to fetch dataset from CKAN API: {response.status_code}")
        except Exception as e:
            self.logger.exception(f"An error occurred while collecting data from CKAN API: {e}")

    def download_and_upload(self, resource_url, hdfs_dir):
        """
        Downloads data from URL and uploads it to HDFS.
        """
        try:
            response = requests.get(resource_url)
            if response.status_code == 200:
                data_bytes = response.content
                filename = resource_url.split('/')[-1]
                self.upload_file_to_hdfs(filename, data_bytes, hdfs_dir)
                self.logger.info(f"Data from CKAN API uploaded to HDFS successfully: {filename}")
            else:
                self.logger.error(f"Failed to download data from URL: {resource_url}, Status code: {response.status_code}")
        except Exception as e:
            self.logger.exception(f"An error occurred while downloading and uploading data: {e}")