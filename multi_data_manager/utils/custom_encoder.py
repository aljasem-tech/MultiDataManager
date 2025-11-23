import json
from datetime import datetime


class CustomEncoder(json.JSONEncoder):
    """
    CustomEncoder: A custom JSON encoder to handle special cases when serializing objects to JSON.
    """

    @staticmethod
    def convert_basic_types(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, float):
            return int(obj) if obj.is_integer() else obj
        return obj

    def convert_list(self, obj):
        return [self.convert_to_dict(item) for item in obj if item not in [None, {}, [], '']]

    def convert_dict(self, obj):
        return {key: self.convert_to_dict(value) for key, value in obj.items() if value not in [None, {}, [], '']}

    def convert_annotated(self, obj):
        return {key: self.convert_to_dict(getattr(obj, key))
                for key in obj.__annotations__ if getattr(obj, key) not in [None, {}, [], '']}

    def convert_to_dict(self, obj):
        """
        ConvertToDict: Converts object attributes into dictionary format, handling special cases.
        """
        if obj is None:
            return None
        if isinstance(obj, (int, str, bool, datetime)):
            return self.convert_basic_types(obj)
        if isinstance(obj, list):
            return self.convert_list(obj)
        if isinstance(obj, dict):
            return self.convert_dict(obj)
        if hasattr(obj, '__annotations__'):
            return self.convert_annotated(obj)

        return obj

    def default(self, o):
        """
        Default: Encodes object to JSON, utilizing convert_to_dict for special cases.
        """
        return self.convert_to_dict(o)
