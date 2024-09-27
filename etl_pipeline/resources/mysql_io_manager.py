import polars as pl
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from dagster import IOManager, OutputContext, InputContext
from contextlib import contextmanager
import logging
import pandas as pd

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@contextmanager
def connect_mysql(config):
    # have some error No module named 'MySQLdb', so I add pymysql to fix it
    conn_info = (
        f"mysql+pymysql://{config['user']}:{config['password']}"
        + f"@{config['host']}:{config['port']}"
        + f"/{config['database']}"
    )
    db_conn = create_engine(conn_info)
    
    try:
        yield db_conn
    except SQLAlchemyError as e:
        logger.error("SQLAlchemyError occurred: %s", e)
        raise
    except Exception as e:
        logger.error("An error occurred: %s", e)
        raise
    finally:
        db_conn.dispose() 

class MySQLIOManager(IOManager):
    def __init__(self, config):
        self._config = config
    def handle_output(self, context: OutputContext, obj: pl.DataFrame):
        pass
    def load_input(self, context: InputContext) -> pl.DataFrame:
        pass

    def extract_data(self, sql: str) -> pl.DataFrame:
        with connect_mysql(self._config) as db_conn:
            pd_data = pd.read_sql_query(sql, db_conn)
            pl_data = pl.from_pandas(pd_data)
            return pl_data