from dotenv import load_dotenv
from pathlib import Path
from sqlalchemy import Table
from sqlalchemy.engine.base import Engine
from sqlalchemy.sql.selectable import Select
from typing import Literal, TypedDict, Tuple
import datetime
import json
import os
import sqlalchemy as sa
import time

dot_env_path = Path(__file__).parent.parent.parent / ".env"
if dot_env_path.is_file():
    load_dotenv()

MYSQL_HOST = os.environ["MYSQL_HOST"]
MYSQL_USER = os.environ["MYSQL_USER"]
MYSQL_PORT = 3306
MYSQL_PASSWORD = os.environ["MYSQL_PASSWORD"]
MYSQL_DATABASE = os.environ["MYSQL_DATABASE"]
MYSQL_URI = (
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DATABASE}"
)

# Always prefix categories by "mysql_" or "bigquery_"
QUERY_CATEGORIES = ("mysql_trips", "mysql_open", "mysql_cnx", "bigquery_trips")

BQ_PROJECT_ID = os.environ["BQ_PROJECT_ID"]
BQ_CREDENTIALS_INFO = json.loads(os.environ["GCP_KEY_TZANAKIS_BIGQUERY"])


class QueryResult(TypedDict):
    id: int
    booking_code: str


def time_function(func):
    """Decorator that logs the elapsed time of a function"""

    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        elapsed_seconds = round(time.time() - start, 2)
        print(f"Elapsed: {elapsed_seconds}")
        return result

    return wrapper


def get_timestamp() -> str:
    now = (
        datetime.datetime.now()
        .replace(microsecond=0, tzinfo=datetime.timezone.utc)
        .isoformat()
    )
    return now


def get_table(engine: Engine, schema, table) -> Table:
    return sa.Table(
        table,
        sa.MetaData(),
        autoload=True,
        autoload_with=engine,
        schema=schema,
    )


class QueryHandler:
    def __init__(
        self, query_category: Literal[QUERY_CATEGORIES], id_range: Tuple[int, int]
    ) -> None:
        self._query_category = query_category
        self._id_range = id_range
        self.init_result()

        self._engine = self.get_engine()
        self._query = self.get_query()

    def init_result(self) -> None:
        self.result = None
        self.result_count_ids = None
        self.result_count_booking_codes = None
        self.updated_at = None

    def update(self) -> None:
        now = get_timestamp()
        result = self.execute_query()
        self.result = result
        self.result_count_ids = len(result)
        self.result_count_booking_codes = len(set(result.values()))
        self.updated_at = now

    @property
    def query_category(self):
        return self._query_category

    @query_category.setter
    def query_category(self, new_query_category: str):

        if new_query_category not in QUERY_CATEGORIES:
            raise ValueError(f"Category should be in {QUERY_CATEGORIES}")

        if self.query_category != new_query_category:
            self.init_result()
            self._query_category = new_query_category

    @property
    def id_range(self):
        return self._id_range

    @id_range.setter
    def id_range(self, new_range: Tuple[int, int]):
        id_min = new_range[0]
        id_max = new_range[1]
        if 0 < id_min < id_max:
            self._id_range = new_range
            self.init_result()
        else:
            raise ValueError("Input (a,b) should satisfy 0 < a < b")

    @time_function
    def execute_query(self) -> QueryResult:

        with self._engine.connect() as connection:
            results_proxy = connection.execute(self._query)
            result = QueryResult(results_proxy.fetchall())

        return result

    def get_engine(self) -> Engine:
        if self.query_category.startswith("mysql_"):
            engine = self._engine_mysql()
        elif self.query_category.startswith("bigquery_"):
            engine = self._engine_bigquery()
        else:
            raise ValueError(
                'query_category should be prefixed by "mysql_" or "bigquery_"'
            )
        return engine

    def _engine_mysql(self) -> Engine:

        mysql_uri = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}"
        engine = sa.create_engine(mysql_uri)
        return engine

    def _engine_bigquery(self) -> Engine:

        engine = sa.create_engine(
            f"bigquery://{BQ_PROJECT_ID}",
            location="eu",
            credentials_info=BQ_CREDENTIALS_INFO,
        )

        return engine

    def get_query(self) -> Select:
        if self.query_category == "mysql_trips":
            return self._query_mysql_trips()
        elif self.query_category == "mysql_open":
            return self._query_mysql_open()
        elif self.query_category == "mysql_cnx":
            return self._query_mysql_cnx()
        elif self.query_category == "bigquery_trips":
            return self._query_bigquery_trips()
        else:
            raise ValueError(f'Query category "{self.query_category}" is not valid.')

    def _query_mysql_trips(self) -> Select:

        table_rbt = get_table(self._engine, MYSQL_DATABASE, "reservations_booked_trips")
        table_rb = get_table(self._engine, MYSQL_DATABASE, "reservations_bookings")

        query = (
            sa.select(
                [
                    table_rbt.columns.id,
                    table_rbt.columns.bookingCode,
                ]
            )
            .select_from(
                table_rbt.join(
                    table_rb,
                    table_rbt.columns.bookingCode == table_rb.columns.orderId,
                    isouter=True,
                )
            )
            .where(
                (table_rb.columns.bookingState == "BOOKED")
                & (table_rbt.columns.replacedBy.is_(None))
                & (table_rbt.columns.id.between(*self.id_range))
            )
        )

        return query

    def _query_mysql_open(self) -> Select:

        table_ror = get_table(
            self._engine, MYSQL_DATABASE, "reservations_open_requests"
        )
        table_rb = get_table(self._engine, MYSQL_DATABASE, "reservations_bookings")

        query = (
            sa.select(
                [
                    table_ror.columns.bookedTripId,
                    table_rb.columns.orderId,
                ]
            )
            .select_from(
                table_ror.join(
                    table_rb,
                    table_ror.columns.bookingId == table_rb.columns.id,
                    isouter=True,
                )
            )
            .where((table_ror.columns.bookedTripId.between(*self.id_range)))
        )

        return query

    def _query_mysql_cnx(self) -> Select:

        table_rr = get_table(self._engine, MYSQL_DATABASE, "reservations_reservations")
        table_rbt = get_table(self._engine, MYSQL_DATABASE, "reservations_booked_trips")
        table_rcr = get_table(
            self._engine, MYSQL_DATABASE, "reservations_cancellation_requests"
        )

        query = (
            sa.select(
                [
                    table_rbt.columns.id,
                    table_rbt.columns.bookingCode,
                ]
            )
            .select_from(
                table_rcr.join(
                    table_rr,
                    (table_rcr.columns.reservationId == table_rr.columns.id),
                    isouter=True,
                ).join(
                    table_rbt,
                    (table_rbt.columns.bookingCode == table_rr.columns.bookingCode)
                    & (
                        table_rbt.columns.ReservationIndex
                        == table_rr.columns.bookingIndex
                    ),
                    isouter=True,
                )
            )
            .where(
                (table_rbt.columns.replacedBy.is_(None))
                & (table_rbt.columns.id.between(*self.id_range))
            )
        )
        return query

    def _query_bigquery_trips(self) -> Select:

        table_brt = get_table(
            self._engine, "booking_documents_production", "base_reporting_trips"
        )

        query = sa.select(
            table_brt.columns.trip_id_trip,
            table_brt.columns.booking_code,
        ).where(
            (table_brt.columns.booking_meta_state == "BOOKED")
            & (table_brt.columns.trip_id_trip.between(*self.id_range))
        )

        return query


if __name__ == "__main__":
    id_min = 1354900
    range_ = (id_min, id_min + 1000)

    handler = QueryHandler("mysql_cnx", range_)
    pass
