from dotenv import load_dotenv
from pathlib import Path
from sqlalchemy.engine.base import Engine
from typing import Literal, TypedDict, Optional, Tuple
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
TRIP_ID_MIN = 1800000
TRIP_ID_MAX = 1928302

QUERY_CATEGORIES_MYSQL = ("mysql_trips", "mysql_open", "mysql_cnx")
QUERY_CATEGORIES_BQ = ("bq_trips",)
QUERY_CATEGORIES = QUERY_CATEGORIES_BQ + QUERY_CATEGORIES_MYSQL

BQ_PROJECT_ID = os.environ["BQ_PROJECT_ID"]
BQ_CREDENTIALS_INFO = json.loads(
    os.environ["GCP_KEY_TZANAKIS_BIGQUERY"]
)


class QueryResult(TypedDict):
    trip_id: int
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


class QueryHandler:
    """
    This class is used to further process the form responses data frame and generate invoices.

    Args:
        category (QUERY_CATEGORIES): One of predefined categories, like "mysql_trips", "bq_trips", etc; see definition of QUERY_CATEGORIES
        trip_id_range (Tuple[int, int]): A tuple of the form (trip_id_min, trip_id_max)

    Attributes:
        category (QUERY_CATEGORIES): See definition in Args
        trip_id_range (Tuple[int, int]): See definition in Args
        results (Optional[QueryResult]): Dictionary where key/value is id/FH code
    """

    def __init__(
        self, category: Literal[QUERY_CATEGORIES], trip_id_range: Tuple[int, int]
    ) -> None:

        self.category = category
        self.trip_id_range = trip_id_range
        self.result = None

        self._trip_id_min = trip_id_range[0]
        self._trip_id_max = trip_id_range[1]
        self._engine = self._get_engine()
        self._query = self._get_query()
        self._updated_at = None
        self._filename = self.get_output_filename()

    def get_output_filename(self):
        if not self.result:
            return None

        filename = f"{self._updated_at}_{self.category}_{self._trip_id_min:09}-{self._trip_id_max:09}.pickle"
        return filename

    @property
    def category(self):
        return self._category

    @category.setter
    def category(self, new_category):
        if not new_category in QUERY_CATEGORIES:
            raise ValueError(f"Category should be in {QUERY_CATEGORIES}")
        self._category = new_category
        self.result = None
        self._filename = self.get_output_filename()

    @property
    def trip_id_range(self):
        return self._trip_id_range

    @trip_id_range.setter
    def trip_id_range(self, new_range: Tuple[int, int]):
        trip_id_min = new_range[0]
        trip_id_max = new_range[1]
        if 0 < trip_id_min < trip_id_max:
            self._trip_id_range = new_range
            self._trip_id_min = trip_id_min
            self._trip_id_max = trip_id_max
            self._filename = self.get_output_filename()
            self.result = None
        else:
            raise ValueError("Input (a,b) should satisfy 0 < a < b")

    def _get_engine(self):
        if self.category in QUERY_CATEGORIES_MYSQL:
            engine_mysql = sa.create_engine(MYSQL_URI)
            return engine_mysql
        else:
            engine_bq = sa.create_engine(
                f"bigquery://{BQ_PROJECT_ID}", location="eu", credentials_info=BQ_CREDENTIALS_INFO
            )
            return engine_bq

    def _get_query(self):

        query = None

        if self.category in QUERY_CATEGORIES_MYSQL:

            table_rb = sa.Table(
                "reservations_bookings",
                sa.MetaData(),
                autoload=True,
                autoload_with=self._engine,
            )

            table_rbt = sa.Table(
                "reservations_booked_trips",
                sa.MetaData(),
                autoload=True,
                autoload_with=self._engine,
            )

            table_ror = sa.Table(
                "reservations_open_requests",
                sa.MetaData(),
                autoload=True,
                autoload_with=self._engine,
            )

            table_rcr = sa.Table(
                "reservations_cancellation_requests",
                sa.MetaData(),
                autoload=True,
                autoload_with=self._engine,
            )

            table_rr = sa.Table(
                "reservations_reservations",
                sa.MetaData(),
                autoload=True,
                autoload_with=self._engine,
            )

            if self.category == "mysql_trips":

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
                        & (
                            table_rbt.columns.id.between(
                                self._trip_id_min, self._trip_id_max
                            )
                        )
                    )
                )

            elif self.category == "mysql_open":

                query = (
                    sa.select(
                        [table_ror.columns.bookedTripId, table_rb.columns.orderId]
                    )
                    .select_from(
                        table_ror.join(
                            table_rb,
                            table_ror.columns.bookingId == table_rb.columns.id,
                            isouter=True,
                        )
                    )
                    .where(
                        (
                            table_ror.columns.bookedTripId.between(
                                self._trip_id_min, self._trip_id_max
                            )
                        )
                    )
                )

            elif self.category == "mysql_cnx":

                query = (
                    sa.select([table_rbt.columns.id, table_rbt.columns.bookingCode])
                    .select_from(
                        table_rcr.join(
                            table_rr,
                            table_rcr.columns.reservationId == table_rr.columns.id,
                            isouter=True,
                        ).join(
                            table_rbt,
                            (
                                table_rbt.columns.bookingCode
                                == table_rr.columns.bookingCode
                            )
                            & (
                                table_rbt.columns.ReservationIndex
                                == table_rr.columns.bookingIndex
                            ),
                            isouter=True,
                        )
                    )
                    .where(
                        (table_rbt.columns.replacedBy.is_(None))
                        & (
                            table_rbt.columns.id.between(
                                self._trip_id_min, self._trip_id_max
                            )
                        )
                    )
                )

        else:
            # self.category in QUERY_CATEGORIES_BQ
            # self.category = "bq_trips"

            table_base_reporting_trips = sa.Table(
                "booking_documents_production.base_reporting_trips",
                sa.MetaData(bind=self._engine),
                autoload=True,
            )
            query = sa.select(
                [sa.func.max(table_base_reporting_trips.columns.trip_id_trip)],
                from_obj=table_base_reporting_trips,
            )

        return query

    @category.setter
    def category(self, new_category):
        if not new_category in QUERY_CATEGORIES:
            raise ValueError(f"Category should be in {QUERY_CATEGORIES}")
        self._category = new_category
        self.result = None

    def _update_updated_at(self):
        now = (
            datetime.datetime.now()
            .replace(microsecond=0, tzinfo=datetime.timezone.utc)
            .isoformat()
        )
        self._updated_at = now

    @time_function
    def update(self):

        with self._engine.connect() as connection:
            results_proxy = connection.execute(self._query)
            self.result = dict(results_proxy.fetchall())
            self._update_updated_at()

if __name__ == "__main__":

    foo = QueryHandler("bq_trips", (1350000, 1355000) )
    pass