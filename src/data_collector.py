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

    def upload_file_to_hdfs(self, filepath, data_bytes, hdfs_dir_path):
        try:
            hdfs_file_path = os.path.join(hdfs_dir_path, os.path.basename(filepath)).replace('\\', '/')
            with self.client.write(hdfs_file_path, overwrite=True) as writer:
                writer.write(data_bytes)

            filepath = filepath.replace('\\', '/')
            self.logger.info(f"File {filepath} uploaded to {hdfs_file_path} successfully.")
        except Exception as e:
            self.client.close()
            self.logger.exception(e)

class JSONCollector(DataCollector):
    def __init__(self, temporal_landing_dir, hdfs_host, hdfs_port, hdfs_user, logger):
        super().__init__(temporal_landing_dir, hdfs_host, hdfs_port, hdfs_user, logger)

    def collect_data(self, url):
        try:
            response = requests.get(url)
            data = response.json()
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            filepath = os.path.join(self.temporal_landing_dir, f'data_{timestamp}.json')
            with open(filepath, 'w') as json_file:
                json.dump(data, json_file)
            self.upload_file_to_hdfs(filepath, json.dumps(data).encode('utf-8'), self.temporal_landing_dir)
        except Exception as e:
            self.client.close()
            self.logger.exception(e)

class CSVCollector(DataCollector):
    def __init__(self, temporal_landing_dir, hdfs_host, hdfs_port, hdfs_user, logger):
        super().__init__(temporal_landing_dir, hdfs_host, hdfs_port, hdfs_user, logger)

    def collect_data(self, url):
        try:
            response = requests.get(url)
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            filepath = os.path.join(self.temporal_landing_dir, f'data_{timestamp}.csv')
            with open(filepath, 'wb') as csv_file:
                csv_file.write(response.content)
            self.upload_file_to_hdfs(filepath, response.content, self.temporal_landing_dir)
        except Exception as e:
            self.client.close()
            self.logger.exception(e)