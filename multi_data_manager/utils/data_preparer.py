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
        """
        return json.dumps(data, cls=CustomEncoder, ensure_ascii=False, indent=indent)
