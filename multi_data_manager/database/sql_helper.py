import functools
import time
from contextlib import contextmanager
from typing import Optional, Any, List, Union, Tuple

import mysql.connector
import pandas as pd
import pyodbc
from mysql.connector import errorcode
from sqlalchemy import create_engine

from multi_data_manager.core.exceptions import DatabaseError
from multi_data_manager.core.logger import logger
from multi_data_manager.utils.data_analyzer import DataAnalyzer


def retry_on_lock_error(retries=3, delay=2):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(retries):
                try:
                    return func(*args, **kwargs)
                except mysql.connector.Error as error:
                    if error.errno in {errorcode.ER_LOCK_WAIT_TIMEOUT, errorcode.ER_LOCK_DEADLOCK}:
                        if attempt < retries - 1:
                            time.sleep(delay)
                        else:
                            raise DatabaseError(f"Lock error after retries: {error}")
                    else:
                        raise DatabaseError(f"MySQL error: {error}")
                except Exception as e:
                    raise DatabaseError(f"Database error: {e}")
            return None

        return wrapper

    return decorator


class SQLHelper:
    """
    Helper class for SQL databases (MySQL, SQL Server).
    """
    DB_MYSQL = 'mysql'
    DB_SQL_SERVER = 'sql_server'

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

    @contextmanager
    def get_cursor(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def _establish_connection(self):
        try:
            if self.db_type == self.DB_MYSQL:
                self.connection = mysql.connector.connect(
                    host=self.connection_info.host,
                    user=self.connection_info.username,
                    password=self.connection_info.password,
                    database=self.connection_info.database,
                    autocommit=False,
                    allow_local_infile=True
                )
            elif self.db_type == self.DB_SQL_SERVER:
                connection_string = (
                    f'DRIVER={{FreeTDS}};'
                    f'SERVER={self.connection_info.host};'
                    f'PORT=1433;'
                    f'DATABASE={self.connection_info.database};'
                    f'UID={self.connection_info.username};'
                    f'PWD={self.connection_info.password};'
                    f'TDS_Version=7.4;'
                )
                self.connection = pyodbc.connect(connection_string)
            else:
                raise DatabaseError(f"Unsupported database type: {self.db_type}")
        except Exception as e:
            logger.error(f"Failed to connect to {self.db_type}: {e}")
            raise DatabaseError(f"Connection failed: {e}")

    def close_connection(self):
        if self.connection:
            try:
                if self.db_type == self.DB_MYSQL and self.connection.is_connected():
                    self.connection.close()
                elif self.db_type == self.DB_SQL_SERVER and not self.connection.closed:
                    self.connection.close()
                logger.info(f'{self.db_type} connection closed.')
            except Exception as e:
                logger.error(f"Error closing connection: {e}")

    @retry_on_lock_error()
    def execute_query(self, query: str, params: Optional[Union[List, Tuple]] = None, execute_many: bool = False,
                      batch_size: int = 100000) -> Optional[List[Any]]:
        rows = None
        try:
            with self.get_cursor() as cursor:
                if self.db_type == self.DB_MYSQL:
                    cursor.execute(f'USE {self.connection_info.database};')

                if params:
                    if execute_many:
                        for i in range(0, len(params), batch_size):
                            batch_data = params[i:i + batch_size]
                            cursor.executemany(query, batch_data)
                    else:
                        cursor.execute(query, params)
                else:
                    cursor.execute(query)

                if cursor.description:
                    rows = cursor.fetchall()
        except Exception as e:
            self.get_connection().rollback()
            logger.error(f"Error executing query: {query} - {e}")
            raise DatabaseError(f"Query execution failed: {e}")

        return rows

    @retry_on_lock_error()
    def create_table(self, table_name: str, json_file_name: str, node: str):
        try:
            with self.get_cursor() as cursor:
                cursor.execute(f'DROP TABLE IF EXISTS `{table_name}`;')

                table_columns, table_indexes = DataAnalyzer.extract_table_info(json_file_name, node)
                needed_columns = ', '.join([f'{col} {col_type}' for col, col_type in table_columns.items()])
                needed_indexes = ', '.join([f'INDEX ({index})' for index in table_indexes]) if table_indexes else ''

                query = f'CREATE TABLE `{table_name}` ({needed_columns}' + (
                    f', {needed_indexes}' if needed_indexes else '') + ');'
                cursor.execute(query)
        except Exception as e:
            logger.error(f"Error creating table {table_name}: {e}")
            raise DatabaseError(f"Create table failed: {e}")

    def query_to_dataframe(self, query: str, params=None, dtype=None) -> pd.DataFrame:
        try:
            # Construct connection string for SQLAlchemy
            if self.db_type == self.DB_MYSQL:
                conn_str = (f'mysql+pymysql://{self.connection_info.username}:'
                            f'{self.connection_info.password}@{self.connection_info.host}:'
                            f'3306/{self.connection_info.database}')
                engine = create_engine(conn_str)
                return pd.read_sql(query, engine, params=params, coerce_float=False, dtype=dtype)
            else:
                # Fallback for other types or implement specific logic
                return pd.read_sql(query, self.get_connection(), params=params, coerce_float=False, dtype=dtype)
        except Exception as e:
            logger.error(f"Error converting query to dataframe: {e}")
            raise DatabaseError(f"DataFrame conversion failed: {e}")

    def dataframe_to_table(self, df: pd.DataFrame, table_name: str, if_exists: str = 'replace', index: bool = False):
        try:
            # Using SQLAlchemy engine for pandas to_sql
            if self.db_type == self.DB_MYSQL:
                conn_str = (f'mysql+pymysql://{self.connection_info.username}:'
                            f'{self.connection_info.password}@{self.connection_info.host}:'
                            f'3306/{self.connection_info.database}')
                engine = create_engine(conn_str)
                df.to_sql(table_name, con=engine, if_exists=if_exists, index=index, method='multi')
            else:
                raise NotImplementedError("dataframe_to_table only implemented for MySQL via SQLAlchemy in this helper")
        except Exception as e:
            logger.error(f"Error saving dataframe to table: {e}")
            raise DatabaseError(f"DataFrame save failed: {e}")
