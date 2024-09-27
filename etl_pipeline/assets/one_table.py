import polars as pl
from resources.mysql_io_manager import MySQLIOManager
from dagster import asset, Output, Definitions, AssetIn, multi_asset, AssetOut

from resources.minio_io_manager import MinIOIOManager
from resources.psql_io_manager import PostgreSQLIOManager



@asset(
    description="Get data olist_orders_dataset from MySql and load into Minio",
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecom"],
    compute_kind="MySQL"
)
def bronze_olist_orders_dataset(context) -> Output[pl.DataFrame]:
        sql_stm = "SELECT * FROM olist_orders_dataset"

        pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)

        return Output(
            pd_data,
            metadata={
                "table": "olist_orders_dataset",
                "records count": len(pd_data),
                "columns": pd_data.columns,
            },
        )

@asset(
    description="Get olist_order_payments_dataset from MySql and load into Minio",
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecom"],
    compute_kind="MySQL"  
)
def bronze_olist_order_payment_dataset(context) -> Output[pl.DataFrame]:
        sql_stm = "SELECT * FROM olist_order_payments_dataset"

        pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)

        return Output(
            pd_data,
            metadata={
                "table": "olist_order_payments_dataset",
                "records count": len(pd_data),
                "columns": pd_data.columns,
            },
        )

@multi_asset(
    description="Load from Mysql to Postgresql",
    ins={
        "bronze_olist_orders_dataset": AssetIn(
            key_prefix=["bronze", "ecom"],
        )
    },
    outs={
        "olist_orders_dataset": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["warehouse", "public"],
            metadata={
                "primary_keys": [
                    "order_id",
                    "customer_id",
                ],

                "columns": [
                    "order_id",
                    "customer_id",
                    "order_status",
                    "order_purchase_timestamp",
                    "order_approved_at",
                    "order_delivered_carrier_date",
                    "order_delivered_customer_date",
                    "order_estimated_delivery_date",
                ],
            },
        )
    },
    compute_kind="PostgreSQL"
)

def olist_orders_dataset(bronze_olist_orders_dataset) ->Output[pl.DataFrame]:
    try:
        return Output(
            value=bronze_olist_orders_dataset,
            metadata={
                "table": "bronze_olist_orders_dataset",
                "records counts": len(bronze_olist_orders_dataset),
            },
        )
    except Exception as e:
         print("--------------", e)


PSQL_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "postgres",
    "user": "admin",
    "password": "admin123",
}
MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "database": "testing",
    "user": "admin",
    "password": "admin123",
}
MINIO_CONFIG = {
    "endpoint_url": "localhost:9000",
    "bucket": "warehouse",
    "aws_access_key_id": "minio",
    "aws_secret_access_key": "minio123",
}

defs = Definitions(
    assets=[bronze_olist_orders_dataset, bronze_olist_order_payment_dataset],
    resources={
        "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
        "psql_io_manager": PostgreSQLIOManager(PSQL_CONFIG),
    },
)