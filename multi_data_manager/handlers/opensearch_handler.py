import json
import threading
from typing import List, Dict, Any, Optional, Union

import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection, helpers
from requests_aws4auth import AWS4Auth

from multi_data_manager.core.logger import logger


class OpensearchHandler:
    """
    A class to handle OpenSearch operations, including batch uploads and queries.
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super(OpensearchHandler, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self, host: str, index: str, role_arn: str, region: str = 'eu-central-1', timeout: int = 30,
                 pool_maxsize: int = 20, pool_connections: int = 20):
        """
        Initialize the OpensearchHandler with connection parameters.

        Args:
            host (str): The OpenSearch host URL.
            index (str): The default index name.
            role_arn (str): The AWS IAM Role ARN for authentication.
            region (str): The AWS region.
            timeout (int): The timeout for OpenSearch requests.
            pool_maxsize (int): The maximum size of the connection pool.
            pool_connections (int): The number of connection pools to cache.
        """
        if self._initialized:
            return

        self.host = host
        self.index = index
        self.role_arn = role_arn
        self.region = region

        sts_client = boto3.client('sts')
        assumed_role = sts_client.assume_role(RoleArn=self.role_arn,
                                              RoleSessionName='AssumedRoleSession')['Credentials']

        auth = AWS4Auth(assumed_role['AccessKeyId'],
                        assumed_role['SecretAccessKey'],
                        self.region,
                        'es',
                        session_token=assumed_role['SessionToken'])

        self.es = OpenSearch(connection_class=RequestsHttpConnection,
                             hosts=[{'host': self.host, 'port': 443}],
                             http_auth=auth,
                             http_compress=True,
                             http_method='POST',
                             use_ssl=True,
                             use_ssl_context=False,
                             verify_certs=True,
                             pool_maxsize=pool_maxsize,
                             pool_connections=pool_connections,
                             pool_block=True,
                             timeout=timeout)

        self._initialized = True

    def _close_connection(self):
        try:
            self.es.transport.close()
            logger.info('OpenSearch connection closed successfully.')
        except Exception as e:
            logger.error(f'Error in OpensearchHandler._close_connection: {e}')
            raise

    def batch_upload(self, documents: Dict, index: Optional[str] = None, recreate_index: bool = False,
                     max_size_mb: int = 10):
        """
        Uploads multiple documents to the OpenSearch index in dynamically sized batches.
        documents must be a dict where the keys are the document IDs.

        Args:
            documents (Dict): A dictionary of documents to upload, with document IDs as keys.
            index (Optional[str]): The target index name. Defaults to the instance's index.
            recreate_index (bool): Whether to recreate the index before uploading.
            max_size_mb (int): The maximum size of each batch in megabytes.
        """
        max_size_bytes = max_size_mb * 1024 * 1024
        total_success, total_failed = 0, 0

        batch = []
        batch_size = 0
        index = index or self.index

        if recreate_index:
            self.create_index(index)

        if not isinstance(documents, dict):
            raise ValueError("documents must be a dictionary")

        for doc_id, doc in documents.items():
            doc_size = len(json.dumps(doc).encode('utf-8'))

            if doc_size > max_size_bytes:
                logger.error(f"Document {doc_id} exceeds max size limit.")
                total_failed += 1
                continue

            if batch_size + doc_size <= max_size_bytes:
                batch.append({'_index': index, '_id': doc_id, '_source': doc})
                batch_size += doc_size
            else:
                try:
                    success, failed = helpers.bulk(self.es, batch, raise_on_error=False, raise_on_exception=False)
                    total_success += success
                    total_failed += len(failed)
                    logger.info(f'Batch insert: Success={success}, Failed={len(failed)}')
                finally:
                    batch = [{'_index': index, '_id': doc_id, '_source': doc}]
                    batch_size = doc_size

        if batch:
            try:
                success, failed = helpers.bulk(self.es, batch, raise_on_error=False, raise_on_exception=False)
                total_success += success
                total_failed += len(failed)
                logger.info(f'Final batch insert: Success={success}, Failed={len(failed)}')
            finally:
                logger.info(f'Total bulk insert completed. Success: {total_success}, Failed: {total_failed}')

    def create_index(self, index_name: str, number_of_shards: int = 1, number_of_replicas: int = 1):
        """
        Creates a new OpenSearch index with specified settings. If the index already exists, it will be deleted first.

        Args:
            index_name (str): The name of the index to create.
            number_of_shards (int): The number of primary shards for the index.
            number_of_replicas (int): The number of replica shards for the index.
        """
        try:
            if self.es.indices.exists(index=index_name):
                self.es.indices.delete(index=index_name)
                logger.debug(f'Index {index_name} deleted successfully.')

            query = {
                'settings': {
                    'index': {
                        'number_of_shards': number_of_shards,
                        'number_of_replicas': number_of_replicas
                    }
                }
            }
            self.es.indices.create(index=index_name, body=query)
            logger.debug(f'Index {index_name} created successfully.')
        except Exception as e:
            logger.error(f'Error in OpensearchHandler.create_index: {e}')
            raise

    def query_index(self, query_body: Dict, index: Optional[str] = None, params: Optional[Dict] = None) -> Union[
        List[Dict], Any]:
        """
        Queries the OpenSearch index with the provided query body.

        Args:
            query_body (Dict): The query body for the search.
            index (Optional[str]): The target index name. Defaults to the instance's index.
            params (Optional[Dict]): Additional parameters for the search request.

        Returns:
            Union[List[Dict], Any]: The list of documents matching the query or the full response if params are provided.
        """
        try:
            index = index or self.index
            response = self.es.search(index=index, body=query_body, params=params)
            if params:
                return response
            else:
                hits = response.get('hits', {}).get('hits', [])
                return [doc['_source'] for doc in hits] if hits else []
        except Exception as e:
            logger.error(f'Error in OpensearchHandler.query_index: {e}')
            return []

    def scroll(self, scroll_id: str, scroll: str = '2m') -> Dict:
        """
        Continues scrolling through search results using the provided scroll ID.

        Args:
            scroll_id (str): The scroll ID from the previous search response.
            scroll (str): The scroll duration to keep the search context alive.

        Returns:
            Dict: The scroll response containing the next batch of results.
        """
        try:
            return self.es.scroll(scroll_id=scroll_id, scroll=scroll)
        except Exception as e:
            logger.error(f'Error in OpensearchHandler.scroll: {e}')
            return {}

    def update_document(self, object_id: str, field_name: str, new_value: Any, index: Optional[str] = None) -> bool:
        """
        Updates a specific field in a document identified by its ID.

        Args:
            object_id (str): The ID of the document to update.
            field_name (str): The field name to update.
            new_value (Any): The new value to set for the field.
            index (Optional[str]): The target index name. Defaults to the instance's index.

        Returns:
            bool: True if the update was successful, False otherwise.
        """
        try:
            index = index or self.index
            response = self.es.update(
                index=index,
                id=object_id,
                body={'doc': {field_name: new_value}}
            )
            return response.get('result') == 'updated'
        except Exception as e:
            logger.error(f'Error in OpensearchHandler.update_document: {e}')
            return False

    def get_document(self, object_id: str, index: Optional[str] = None) -> Optional[Dict]:
        """
        Retrieves a document by its ID from the specified index.

        Args:
            object_id (str): The ID of the document to retrieve.
            index (Optional[str]): The target index name. Defaults to the instance's index.

        Returns:
            Optional[Dict]: The document source if found, None otherwise.
        """
        try:
            index = index or self.index
            response = self.es.get(index=index, id=object_id)
            return response.get('_source')
        except Exception as e:
            logger.error(f'Error in OpensearchHandler.get_document: {e}')
            return None

    def get_documents_fields(self, object_ids: List[str], fields: List[str], index: Optional[str] = None,
                             size: int = 10000) -> Dict[int, Dict]:
        """
        Retrieves specific fields for multiple documents identified by their IDs.

        Args:
            object_ids (List[str]): A list of document IDs to retrieve.
            fields (List[str]): A list of fields to retrieve from each document.
            index (Optional[str]): The target index name. Defaults to the instance's index.
            size (int): The maximum number of documents to retrieve.

        Returns:
            Dict[int, Dict]: A dictionary mapping document IDs to their corresponding field data.
        """
        try:
            index = index or self.index
            response = self.es.search(
                index=index,
                body={
                    '_source': fields,
                    'query': {'ids': {'values': object_ids}},
                    'size': size
                }
            )
            hits = response.get('hits', {}).get('hits', [])
            # Assuming ID format ends with -<int> as per original code, but adding safety
            results = {}
            for hit in hits:
                try:
                    key = int(hit.get('_id').split('-')[-1])
                    results[key] = hit.get('_source')
                except (ValueError, IndexError):
                    # Fallback if ID format is different
                    results[hit.get('_id')] = hit.get('_source')
            return results
        except Exception as e:
            logger.error(f'Error in OpensearchHandler.get_documents_fields: {e}')
            return {}

    def __del__(self):
        logger.info('Closing OpenSearch connection...')
        self._close_connection()
