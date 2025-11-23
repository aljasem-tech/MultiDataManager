import json

from multi_data_manager.utils.custom_encoder import CustomEncoder


class DataPreparer:
    """
    Utility class to prepare data for upload/export.
    """

    @staticmethod
    def prepare_json(data, indent=None):
        """
        Converts data to a JSON string using CustomEncoder.

        Args:
            data: The data to be converted to JSON.
            indent (int, optional): Number of spaces for indentation. Defaults to None.

        Returns:
            str: The JSON string representation of the data.
        """
        return json.dumps(data, cls=CustomEncoder, ensure_ascii=False, indent=indent)
