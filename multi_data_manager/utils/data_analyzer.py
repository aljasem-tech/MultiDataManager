import argparse
import json
import re
from typing import Dict, List, Tuple, Any


class DataAnalyzer:
    """
    Utility class for analyzing data and file structures.
    """

    @staticmethod
    def get_data_type(file_name: str) -> str:
        """
        Determine the data type based on the file name.
        """
        key_list = re.split(r'[/ \-.]', file_name)
        result = 'Invalid'

        # This list could be configurable in the future
        known_types = ['text', 'SchemaGroupsForVehicle', 'SchemaGroup', 'TecData', 'Vehicle']

        for data_type in known_types:
            if data_type in key_list:
                result = data_type
                break

        return result

    @staticmethod
    def extract_table_info(json_file_name: str, node: str) -> Tuple[Dict[str, str], List[str]]:
        """
        Extract table columns and indexes from a JSON schema file.
        """
        table_columns = dict()
        table_indexes = []

        with open(json_file_name, 'r') as json_file:
            json_file_content = json.load(json_file)

        if node not in json_file_content:
            raise ValueError(f"Node '{node}' not found in {json_file_name}")

        needed_node = json_file_content[node]
        columns = needed_node.get('Columns', {})
        indexes = needed_node.get('Index', [])

        # Extract all columns
        table_columns.update(columns)

        # Extract all indexes
        table_indexes.extend(indexes)

        return table_columns, table_indexes

    @staticmethod
    def str2bool(v: Any) -> bool:
        """
        Convert a string representation of truth to True or False.
        """
        if isinstance(v, bool):
            return v
        if str(v).lower() in ('yes', 'true', 't', 'y', '1'):
            return True
        elif str(v).lower() in ('no', 'false', 'f', 'n', '0'):
            return False
        else:
            raise argparse.ArgumentTypeError('Boolean value expected.')
