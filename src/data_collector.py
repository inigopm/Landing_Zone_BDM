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
        try:
            if self.client.status(folder, strict=False) is None:
                self.client.makedirs(folder)
                self.logger.info(f"Directory {folder} created successfully.")
            else:
                self.logger.info(f"Directory {folder} already exists.")
        except Exception as e:
            self.client.close()
            self.logger.exception(e)

    def collect_data_from_ckan_api(self, dataset_id):
        """
        Collects data from the CKAN API for the given dataset ID and uploads it to HDFS.
        Args:
            dataset_id (str): The ID of the dataset on CKAN.
        Returns:
            None
        """
        try:
            # Define the base URL for the CKAN API
            base_url = "https://opendata-ajuntament.barcelona.cat/data/api/3/"
            # Set up the endpoint URL to access the dataset's resources
            endpoint_url = f"{base_url}action/package_show?id={dataset_id}"
            # Set up the headers for the API request
            headers = {'Content-Type': 'application/json'}
            # Send a GET request to the API endpoint
            response = requests.get(endpoint_url, headers=headers)
            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                # Extract the resources from the response
                resources = response.json()['result']['resources']
                # Loop through each resource
                for resource in resources:
                    # Get the URL of the resource
                    resource_url = resource['url']
                    # Download the resource
                    print(f"Downloading data from CKAN API: {resource_url}...")
                    r = requests.get(resource_url)
                    # Upload the downloaded data to HDFS
                    hdfs_dir_path = self.temporal_landing_dir  # Use temporal landing directory for CKAN data
                    data_bytes = r.content
                    self.upload_file_to_hdfs(resource['name'], data_bytes, hdfs_dir_path)
                    print(f"Data from CKAN API uploaded to HDFS successfully: {resource['name']}")
            else:
                print(f"Failed to fetch dataset from CKAN API: {response.status_code}")
        except Exception as e:
            self.logger.exception(f"An error occurred while collecting data from CKAN API: {e}")