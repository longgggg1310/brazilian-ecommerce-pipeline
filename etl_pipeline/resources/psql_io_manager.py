from contextlib import contextmanager
from datetime import datetime
import polars as pl
from dagster import IOManager, OutputContext, InputContext
from sqlalchemy import create_engine
from psycopg2 import sql
import psycopg2
import psycopg2.extras


@contextmanager
def connect_psql(config):
    conn_info = (
        f"postgresql+psycopg2://{config['user']}:{config['password']}"
        + f"@{config['host']}:{config['port']}"
        + f"/{config['database']}"
        )
    db_conn = create_engine(conn_info)
    connection = db_conn.raw_connection()  
    try:
        yield connection
    except (Exception) as e:
        print(f"Error while connecting to PostgreSQL: {e}")


class PostgreSQLIOManager(IOManager):
    def __init__(self, config):
        self._config = config
    def load_input(self, context: InputContext) -> pl.DataFrame:
        pass
    def handle_output(self, context: "OutputContext", obj: pl.DataFrame):
        schema = context.asset_key.path[-2]
        table = str(context.asset_key.path[-1]).replace("warehouse_", "")
        context.log.debug(f"Schema: {schema}, Table: {table}")
        columns = sql.SQL(", ").join([sql.Identifier(col) for col in obj.columns])
        values = sql.SQL(", ").join([sql.Placeholder()] * len(obj.columns))

        try:
            with connect_psql(self._config) as conn:
                context.log.debug(f"Connected to PostgreSQL: {conn}")
                context.log.debug("Bia33333 ",context.metadata)
                primary_keys = (context.metadata or {}).get("primary_keys", [])
                context.log.debug(f"Primary keys: {primary_keys}")
                with conn.cursor() as cursor:
                    # context.log.debug(f"Cursor info: {cursor}")
                    insert_query = sql.SQL(
                        "INSERT INTO {0}.{1} ({2}) VALUES ({3})"
                    ).format(
                        sql.Identifier(schema),
                        sql.Identifier(table),
                        columns,
                        values
                    )
                    if primary_keys:
                            conflict_columns = sql.SQL(", ").join([sql.Identifier(pk) for pk in primary_keys])
                            update_columns = sql.SQL(", ").join([
                                sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col))
                                for col in obj.columns if col not in primary_keys
                            ])
                            insert_query += sql.SQL(" ON CONFLICT ({}) DO UPDATE SET {}").format(
                                conflict_columns, update_columns
                            )
                    # Execute the batch insert or upsert
                    psycopg2.extras.execute_batch(cursor, insert_query, obj.values.tolist())
                    conn.commit()  # Commit the transaction
        except (Exception) as e:
            print(f"Error while handling output to P2ostgreSQL: {e}")
