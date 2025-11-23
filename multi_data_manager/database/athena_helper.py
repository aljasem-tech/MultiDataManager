import boto3
from pyathena import connect
from typing import Any, Optional, List, Tuple

from multi_data_manager.core.logger import logger
from multi_data_manager.core.exceptions import DatabaseError

class AthenaHelper:
    """
    Helper class for Athena and AWS CLI interactions.
    """
    DB_ATHENA = 'athena'
    DB_AWS_CLI = 'aws_cli'

    def __init__(self, db_type: str, connection_info: Any):
        self.db_type = db_type
        self.connection_info = connection_info
        self.connection = None
        self._establish_connection()

    def __del__(self):
        self.close_connection()

    def get_connection(self):
        if not self.connection:
            self._establish_connection()
        return self.connection

    def _establish_connection(self):
        try:
            boto3.setup_default_session(profile_name=self.connection_info.profile)
            
            if self.db_type == self.DB_ATHENA:
                self.connection = connect(
                    s3_staging_dir=self.connection_info.s3_staging_dir,
                    region_name=self.connection_info.region,
                    schema_name=self.connection_info.database
                )
            elif self.db_type == self.DB_AWS_CLI:
                self.connection = boto3.client(self.connection_info.service)
            else:
                raise DatabaseError(f"Unsupported database type: {self.db_type}")
        except Exception as e:
            logger.error(f"Failed to connect to {self.db_type}: {e}")
            raise DatabaseError(f"Connection failed: {e}")

    def close_connection(self):
        if self.connection:
            try:
                if self.db_type == self.DB_ATHENA:
                    self.connection.close()
                logger.info(f'{self.db_type} connection closed.')
            except Exception as e:
                logger.error(f"Error closing connection: {e}")

    def execute_query(self, query: str, params: Optional[Union[List, Tuple]] = None) -> Optional[List[Any]]:
        """
        Execute a query on Athena.
        """
        if self.db_type != self.DB_ATHENA:
             raise DatabaseError("execute_query is only supported for Athena in this helper.")
        
        rows = None
        try:
            cursor = self.get_connection().cursor()
            cursor.execute(query, params)
            if cursor.description:
                rows = cursor.fetchall()
            cursor.close()
        except Exception as e:
            logger.error(f"Error executing Athena query: {query} - {e}")
            raise DatabaseError(f"Athena query failed: {e}")
        
        return rows
