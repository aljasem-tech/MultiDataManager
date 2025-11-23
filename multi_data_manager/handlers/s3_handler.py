import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Optional, Dict, Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from multi_data_manager.core.constants import MAX_WORKERS
from multi_data_manager.core.logger import logger
from multi_data_manager.utils.data_preparer import DataPreparer


class S3Handler:
    """
    A class to handle S3 operations, including uploading and downloading JSON files.
    """

    def __init__(self, max_pool_connections: int = 100):
        """
        Initializes the S3Handler class and configures the boto3 client.

        Args:
            max_pool_connections (int): Maximum number of connections in the connection pool.
        """
        config = Config(max_pool_connections=max_pool_connections)
        self.s3 = boto3.client('s3', config=config)
        self.data_preparer = DataPreparer()

    def upload_all_to_s3(self, s3_files: List[Tuple[str, Any]], target_s3_bucket: str, s3_prefix: str):
        """
        Uploads multiple JSON files to an S3 bucket concurrently.

        Args:
            s3_files (List[Tuple[str, Any]]): A list of tuples containing file
                names and their corresponding JSON-serializable objects.
            target_s3_bucket (str): The target S3 bucket name.
            s3_prefix (str): The S3 prefix (folder path) where files will be uploaded.
        """

        def upload_file(file_content, file_name):
            self.put_json(file_content, target_s3_bucket, f'{s3_prefix}/{file_name}')

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(upload_file, self.data_preparer.prepare_json(file_content), file_name)
                       for file_name, file_content in s3_files if file_content is not None]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f'Error in S3Handler.upload_file in uploading file: {e}')

    def put_json(self, target_object: Any, bucket_name: str, object_key: str):
        """
        Uploads a JSON object to an S3 bucket.

        Args:
            target_object (Any): The JSON-serializable object to upload.
            bucket_name (str): The target S3 bucket name.
            object_key (str): The S3 object key (file path) where the object will be uploaded.
        """
        # If target_object is not string/bytes, convert it
        if not isinstance(target_object, (str, bytes)):
            target_object = self.data_preparer.prepare_json(target_object)

        self.s3.put_object(Bucket=bucket_name, Key=object_key, Body=target_object)

    def get_json(self, bucket_name: str, object_key: str) -> Optional[Dict]:
        """
        Downloads a JSON object from an S3 bucket.

        Args:
            bucket_name (str): The S3 bucket name.
            object_key (str): The S3 object key (file path) to download.

        Returns:
            Optional[Dict]: The JSON object if the download is successful, None otherwise.
        """
        try:
            response = self.s3.get_object(Bucket=bucket_name, Key=object_key)
            return json.loads(response['Body'].read().decode('utf-8'))
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                logger.error(f'The file {object_key} does not exist in the S3 bucket {bucket_name}.')
            else:
                logger.error(f'An error occurred while downloading the file from S3: {e}')
            return None

    def download_file(self, bucket_name: str, object_key: str, file_path: str):
        """
        Downloads a file from an S3 bucket to a local file path.

        Args:
            bucket_name (str): The S3 bucket name.
            object_key (str): The S3 object key (file path) to download.
            file_path (str): The local file path where the file will be saved.
        """
        self.s3.download_file(bucket_name, object_key, file_path)
