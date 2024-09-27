import os
from contextlib import contextmanager
from datetime import datetime
from typing import Union

import polars as pl
from dagster import IOManager, OutputContext, InputContext
from minio import Minio

# Decorator
@contextmanager #control resource make sure that resource managed and cleaned up
def connect_minio(config):
    client = Minio(
        endpoint=config.get("endpoint_url"),
        access_key=config.get("aws_access_key_id"),
        secret_key=config.get("aws_secret_access_key"),
        secure=False,
    )
    try:
        yield client
    except Exception:
        print(f"An error occurred: {e}")
        raise
    finally: pass

def create_bucket(client: Minio, bucket_name):
        is_exist = client.bucket_exists(bucket_name)
        if not is_exist:
            client.make_bucket(bucket_name)
        print(f"Bucket {bucket_name} already exists.")

class MinIOIOManager(IOManager):
    def __init__(self, config):
        self._config = config
    def _get_path(self, context: Union[InputContext, OutputContext]):
        layer, schema, table = context.asset_key.path

        key = "/".join([layer, schema, table.replace(f"{layer}_", "")])
        tmp_file_path = "/tmp/file-{}-{}.parquet".format(
            datetime.today().strftime("%Y%m%d%H%M%S"),
            "-".join(context.asset_key.path)
            )
        print("----- ", tmp_file_path)
        return f"{key}.pq", tmp_file_path
    
    
    def handle_output(self, context: OutputContext, obj: pl.DataFrame):
        key_name, tmp_file_path = self._get_path(context)
        try:
            bucket_name = self._config.get("bucket")
            obj.write_parquet(tmp_file_path)  # Save DataFrame to temporary parquet file
            with connect_minio(self._config) as client:
                create_bucket(client, bucket_name)
                # Upload to MinIO
                client.fput_object(
                    bucket_name=bucket_name,  
                    object_name=key_name,
                    file_path=tmp_file_path
                )
                context.log.info(f"Successfully uploaded {tmp_file_path} to MinIO as {key_name}.")
                os.remove(tmp_file_path)
        except Exception as e:
            context.log.error(f"Failed to upload to MinIO: {e}")
            raise
        

    def load_input(self, context: InputContext) -> pl.DataFrame:
        # Implement logic to read from MinIO
        key_name, _ = self._get_path(context)
        try:
            bucket_name = self._config.get("bucket")
            # print("====== ", context.asset_key.path)
            with connect_minio(self._config) as client:
                # Download the parquet file to a temporary location
                create_bucket(client, bucket_name)
                tmp_file_path = f"/tmp/{key_name}"
                client.fget_object(
                    bucket_name=bucket_name,  # Assuming the first path component is the bucket name
                    object_name=key_name,
                    file_path=tmp_file_path
                )
                # Load the DataFrame from the parquet file
                df = pl.read_parquet(tmp_file_path)
                return df
        except Exception as e:
            context.log.error(f"Failed to load DataFrame from MinIO: {e}")
            raise
        