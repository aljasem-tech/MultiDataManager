import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Any

from multi_data_manager.core.constants import MAX_WORKERS
from multi_data_manager.core.logger import logger
from multi_data_manager.utils.custom_encoder import CustomEncoder


class FileHandler:
    """
    A class to handle local file operations, including exporting data to JSON files.
    """

    @staticmethod
    def export_to_json(json_object: Any, file_name: str, indent: int = 2) -> str:
        """
        Exports a JSON object to a file with the specified indentation.
        """
        os.makedirs(os.path.dirname(file_name), exist_ok=True)

        json_result = json.dumps(json_object, cls=CustomEncoder, ensure_ascii=False, indent=indent)
        with open(file_name, 'w', encoding='utf-8') as outfile:
            outfile.write(json_result)

        return json_result

    def export_all(self, targeted_files: List[Tuple[str, Any]], source_folder: str, object_data_type: str):
        """
        Exports multiple JSON objects to file concurrently.
        """
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            for object_name, target_object in targeted_files:
                if target_object:
                    if isinstance(target_object, dict):
                        json_data = target_object
                    elif hasattr(target_object, 'model_dump'):
                        json_data = target_object.model_dump(exclude_none=True, exclude_unset=True)
                    else:
                        # Fallback or assume it's serializable
                        json_data = target_object

                    file_path = os.path.join(source_folder, object_data_type, object_name)
                    future = executor.submit(self.export_to_json, json_data, file_path, 2)
                    futures.append(future)
                else:
                    logger.warning(f'No data found for {object_name}')

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f'Error in FileHandler.export_all: {e}')
                    raise
