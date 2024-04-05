from hdfs import InsecureClient
import pandas as pd
from pymongo import MongoClient
from pyarrow import parquet as pq
from dotenv import load_dotenv

env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path=env_path)

# Configurations
HDFS_HOST = os.getenv('HDFS_HOST')
HDFS_USER = os.getenv('HDFS_USER')
MONGO_CONNECTION_STRING = os.getenv('MONGO_CONNECTION_STRING')
MONGO_DB_NAME = os.getenv('MONGO_DB_NAME')
MONGO_COLLECTION_NAME = os.getenv('MONGO_COLLECTION_NAME')

hdfs_client = InsecureClient(HDFS_HOST, user=HDFS_USER)
mongo_client = MongoClient(MONGO_CONNECTION_STRING)
mongo_db = mongo_client[MONGO_DB_NAME]
mongo_collection = mongo_db[MONGO_COLLECTION_NAME]

# Util functions
def load_csv_from_hdfs_to_parquet(hdfs_path, local_path, parquet_path):
    """Download CSV from HDFS, convert to DataFrame and Parquet."""
    hdfs_client.download(hdfs_path, local_path, overwrite=True)
    df = pd.read_csv(local_path)
    table = pq.Table.from_pandas(df)
    pq.write_table(table, parquet_path)

def load_json_from_hdfs_to_mongodb(hdfs_path, local_path):
    """Download JSON from HDFS, and add it to MongoDB."""
    hdfs_client.download(hdfs_path, local_path, overwrite=True)
    with open(local_path, 'r') as f:
        data = pd.read_json(f)
        mongo_collection.insert_many(data.to_dict('records'))

if __name__ == "__main__":
    # Load CSV Data of Open Data and Lookup Tables as Parquet
    open_data_hdfs_path = '/path/to/opendata/csv'
    lookup_tables_hdfs_path = '/path/to/lookup/tables/csv'
    local_csv_path = '/tmp/tempfile.csv'
    parquet_path = '/path/to/output.parquet'
    
    load_csv_from_hdfs_to_parquet(open_data_hdfs_path, local_csv_path, parquet_path)
    load_csv_from_hdfs_to_parquet(lookup_tables_hdfs_path, local_csv_path, parquet_path)

    # Load JSON data of Idealista to MongoDB
    idealista_json_hdfs_path = '/path/to/idealista/json'
    local_json_path = '/tmp/tempfile.json'
    
    load_json_from_hdfs_to_mongodb(idealista_json_hdfs_path, local_json_path)

