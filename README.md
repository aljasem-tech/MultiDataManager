# Multi Data Manager

[![Build and Release](https://github.com/aljasem-tech/MultiDataManager/actions/workflows/build_and_release.yml/badge.svg)](https://github.com/aljasem-tech/MultiDataManager/actions/workflows/build_and_release.yml)
![Python Version](https://img.shields.io/badge/python-3.9%2B-blue)
[![Tests](https://img.shields.io/badge/tests-passing-brightgreen)](https://github.com/aljasem-tech/MultiDataManager/actions)

A unified Python library for data management, database operations, and cloud interactions. This library consolidates
utilities for handling SQL databases (MySQL, SQL Server), AWS services (S3, Athena, OpenSearch), and general data
processing.

## Installation

```bash
pip install .
```

## Features

### Core

- **Unified Logging**: Standardized logging configuration.
- **Custom Exceptions**: Clear error handling with library-specific exceptions.

### Database

- **SQLHelper**: Interact with MySQL and SQL Server databases.
- **AthenaHelper**: Execute queries on AWS Athena.

### Handlers

- **S3Handler**: Upload and download files from AWS S3.
- **OpensearchHandler**: Batch upload and query OpenSearch indices.
- **FileHandler**: Export data to local JSON files.
- **APIHandler**: Invoke external APIs with authentication.

### Utilities

- **DataCleaner**: Clean and sanitize string data.
- **DataAnalyzer**: Extract schema information from JSON files.
- **DocumentationGenerator**: Auto-generate markdown documentation from source code.

## Usage Examples

### Database Operations

```python
from multi_data_manager.database.sql_helper import SQLHelper

# Initialize connection
db_helper = SQLHelper(db_type='mysql', connection_info=my_config)

# Execute query
results = db_helper.execute_query("SELECT * FROM users")
```

### S3 Operations

```python
from multi_data_manager.handlers.s3_handler import S3Handler

s3_handler = S3Handler()
s3_handler.upload_all_to_s3([('data.json', {'key': 'value'})], 'my-bucket', 'prefix')
```

### OpenSearch Operations

```python
from multi_data_manager.handlers.opensearch_handler import OpensearchHandler

os_handler = OpensearchHandler(host='host', index='index', role_arn='arn')
os_handler.batch_upload([{'id': 1, 'data': 'test'}])
```

## Development

### Running Tests

(Add instructions for running tests when available)

### Generating Documentation

```python
from multi_data_manager.utils.docs_generator import DocumentationGenerator

gen = DocumentationGenerator(root_dir='multi_data_manager', destination_dir='docs')
gen.generate_docs()
```
