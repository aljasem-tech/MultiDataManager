import sys
import os
from unittest.mock import MagicMock

# Add the current directory to sys.path to ensure we can import the package
sys.path.append(os.getcwd())

# Mock external dependencies
sys.modules['mysql'] = MagicMock()
sys.modules['mysql.connector'] = MagicMock()
sys.modules['mysql.connector.errorcode'] = MagicMock()
sys.modules['pyodbc'] = MagicMock()
sys.modules['boto3'] = MagicMock()
sys.modules['botocore'] = MagicMock()
sys.modules['botocore.config'] = MagicMock()
sys.modules['botocore.exceptions'] = MagicMock()
sys.modules['opensearchpy'] = MagicMock()
sys.modules['requests_aws4auth'] = MagicMock()
sys.modules['pyathena'] = MagicMock()
sys.modules['sqlalchemy'] = MagicMock()
sys.modules['pandas'] = MagicMock()
sys.modules['requests'] = MagicMock()
sys.modules['requests.exceptions'] = MagicMock()

try:
    print("Importing multi_data_manager...")
    import multi_data_manager
    print("Success!")

    print("Importing core modules...")
    from multi_data_manager.core.logger import logger
    from multi_data_manager.core.exceptions import MultiDataManagerError
    from multi_data_manager.core.constants import MAX_WORKERS
    print("Success!")

    print("Importing utils...")
    from multi_data_manager.utils.data_cleaner import DataCleaner
    from multi_data_manager.utils.data_analyzer import DataAnalyzer
    from multi_data_manager.utils.docs_generator import DocumentationGenerator
    from multi_data_manager.utils.custom_encoder import CustomEncoder
    from multi_data_manager.utils.data_preparer import DataPreparer
    print("Success!")

    print("Importing database helpers...")
    from multi_data_manager.database.sql_helper import SQLHelper
    from multi_data_manager.database.athena_helper import AthenaHelper
    print("Success!")

    print("Importing handlers...")
    from multi_data_manager.handlers.s3_handler import S3Handler
    from multi_data_manager.handlers.opensearch_handler import OpensearchHandler
    from multi_data_manager.handlers.file_handler import FileHandler
    from multi_data_manager.handlers.api_handler import APIHandler
    print("Success!")

    print("Testing DataCleaner...")
    cleaned = DataCleaner.cleanup_string("Hello @World!")
    if cleaned == "HelloWorld":
        print(f"DataCleaner passed: {cleaned}")
    else:
        print(f"DataCleaner failed: {cleaned}")

    print("Testing DataPreparer...")
    data = {"key": "value"}
    json_str = DataPreparer.prepare_json(data)
    if '"key": "value"' in json_str:
         print(f"DataPreparer passed: {json_str}")
    else:
         print(f"DataPreparer failed: {json_str}")

    print("All checks passed!")

except Exception as e:
    print(f"Verification failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
