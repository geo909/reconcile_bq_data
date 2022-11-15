from reconcilebqdata.config import (
    MYSQL_HOST,
    MYSQL_PASSWORD,
    MYSQL_USER,
    BQ_PROJECT_ID,
    BQ_CREDENTIALS_INFO,
)
from reconcilebqdata.aux import time_function
from sqlalchemy import Table
from sqlalchemy.engine.base import Engine
from sqlalchemy.sql.selectable import Select
import sqlalchemy as sa


def get_table(engine: Engine, schema, table) -> Table:
    return sa.Table(
        table,
        sa.MetaData(),
        autoload=True,
        autoload_with=engine,
        schema=schema,
    )


def get_engine_mysql() -> Engine:

    mysql_uri = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}"
    engine = sa.create_engine(mysql_uri)
    return engine


def get_engine_bigquery() -> Engine:

    engine = sa.create_engine(
        f"bigquery://{BQ_PROJECT_ID}",
        location="eu",
        credentials_info=BQ_CREDENTIALS_INFO,
    )

    return engine


@time_function
def execute_query(engine: Engine, query: Select) -> list:

    with engine.connect() as connection:
        results_proxy = connection.execute(query)
        result = results_proxy.fetchall()

    return result
