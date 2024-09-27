# import os

# from etl_pipeline.resources.minio_io_manager import MinIOIOManager
# from etl_pipeline.resources.mysql_io_manager import MySQLIOManager
# from etl_pipeline.resources.psql_io_manager import PostgreSQLIOManager
# from dagster import Definitions, load_assets_from_modules
# from . import assets


# PSQL_CONFIG = {
#     "host": "localhost",
#     "port": 5432,
#     "database": "postgres",
#     "user": "admin",
#     "password": "admin123",
# }
# MYSQL_CONFIG = {
#     "host": "localhost",
#     "port": 3306,
#     "database": "testing",
#     "user": "admin",
#     "password": "admin123",
# }
# MINIO_CONFIG = {
#     "endpoint_url": "localhost:9000",
#     "bucket": "warehouse",
#     "aws_access_key_id": "minio",
#     "aws_secret_access_key": "minio123",
# }

# resources = {
#     "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
#     "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
#     "psql_io_manager": PostgreSQLIOManager(PSQL_CONFIG),
# }

# defs = Definitions(
#     assets=load_assets_from_modules([assets]),
#     resources={
#         "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
#         "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
#         "psql_io_manager": PostgreSQLIOManager(PSQL_CONFIG),
#     },
# )