from setuptools import setup, find_packages

setup(
    name="multi_data_manager",
    version="0.2.0",
    description="A unified library for data management, database operations, and cloud interactions.",
    author="User",
    packages=find_packages(),
    install_requires=[
        "boto3",
        "mysql-connector-python",
        "pyodbc",
        "pandas",
        "requests",
        "pyathena",
        "sqlalchemy",
        "pymysql"
    ],
    python_requires=">=3.6",
)
