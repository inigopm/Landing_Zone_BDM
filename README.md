# Big Data Management - Universitat Politècnica de Catalunya
## Implementation of a Data Management Backbone

### Instructions for Executing the Code

- Create a virtual environment with Python 3.10 interpreter and run the following command to install the required libraries:
  - ```
      pip install requirements.txt
    ```
- Add a `.env` file inside BDM folder, including the following parameters with their appropriate values:
  - ```
    HDFS_HOST='10.4.41.49' #Change this
    HDFS_PORT=9870
    HDFS_USER='bdm' #Change this
    TEMPORAL_LANDING_DIR_PATH = '/temporal_landing'
    PERSISTENT_LANDING_DIR_PATH = '/persistent_landing'
    MONGO_DB_URL = '10.4.41.49' #Change this
    MONGO_DB_PORT = 27017
    MONGO_DB_NAME = 'bdm' #Change this
    MONGO_COLLECTION_NAME = 'idealista'
    ```
- For executing `data_collector` functionality in order to move data from the local machine or the Open Data BCN API to HDFS, make use of the following command:
  - ``` 
    python3 main.py data_collector
    ```
- Otherwise, for executing `data_loader` functionality in order to move the available data of the `Temporal Landing Zone` to the `Persistence Landing Zone`, use the following command:
  - ```
    python3 main.py data_loader
    ```