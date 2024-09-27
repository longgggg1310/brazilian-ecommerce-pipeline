# import polars as pl
# from dagster import asset, AssetIn, Output, StaticPartitionsDefinition
# from datetime import datetime

# COMPUTE_KIND = "MySQL"
# LAYER = "bronze"


# ls_tables = [
#     "olist_order_items_dataset",
#     "olist_order_payments_dataset",
#     "olist_orders_dataset",
#     "olist_products_dataset",
#     "product_category_name_translation",
# ]



# @asset(
#     description="Get olist_order_items_dataset from MySql and load into Minio",
#     io_manager_key="minio_io_manager",
#     required_resource_keys={"mysql_io_manager"},
#     key_prefix=["bronze", "ecom"],
#     compute_kind= COMPUTE_KIND,
#     group_name = LAYER
# )
# def bronze_olist_orders_dataset(context) -> Output[pl.DataFrame]:
#     sql_stm = "SELECT * FROM olist_orders_dataset"

#     pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)

#     return Output(
#         pd_data,
#         metadata={
#             "table": "olist_orders_dataset",
#             "records count": len(pd_data),
#         },
#     )
