from reconcilebqdata.aux import get_current_timestamp, time_function
from reconcilebqdata.config import MYSQL_DATABASE
from reconcilebqdata.dbtools import (
    get_table,
    get_engine_mysql,
    get_engine_bigquery,
    execute_query,
)
from sqlalchemy.engine.base import Engine
from sqlalchemy.sql.selectable import Select
from typing import Literal, TypedDict, Tuple
from datetime import date
import sqlalchemy as sa

# Always prefix categories by "mysql_" or "bigquery_"
QUERY_CATEGORIES = ("mysql_trips", "mysql_open", "mysql_cnx", "bigquery_trips")


@time_function
def get_bq_trip_id_range(date_min: date, date_max: date) -> tuple[int, int]:
    """Helper function to be used for initializing an instance of a QueryHandler class.
    This will translate a date range into a trip id range.

    Args:
        date_min: Start date
        date_max: End date

    Returns:
        A 2-tuple of the form (id_min, id_max)

    """

    if date_min >= date_max:
        raise ValueError("Invalid date range")

    engine = get_engine_bigquery()

    table = get_table(engine, "booking_documents_production", "base_reporting_trips")

    query = sa.select(
        sa.func.min(table.columns.trip_id_trip),
        sa.func.max(table.columns.trip_id_trip),
    ).where(
        (table.columns.booking_meta_state == "BOOKED")
        & (table.columns.booking_date.between(date_min, date_max))
    )

    id_range = execute_query(engine, query)[0]

    return id_range


class QueryResult(TypedDict):
    id: int
    booking_code: str


class QueryHandler:

    """
    Class to handle a predefined query in MySQL or BigQuery. Given an id range and a category, this will return
    a dictionary of ids and booking codes that are the result of such query.

    Usage: define a QueryHandler object, run the update method and access the results in the result attribute.

    Args (pd.DataFrame):
        query_category (Literal[QUERY_CATEGORIES]): A string among the elements of QUERY_CATEGORIES
        id_range (Tuple[int, int]): A 2-tuple of the form (id_min, id_max)

    Attributes:
        _query_category (Literal[QUERY_CATEGORIES]): See query_category in args
        _id_range (Tuple[int, int]): See id_range in args
        _engine (Engine): sql alchemy engine object which either points to our MySQL or BigQuery
        _query (Select): An sqlalchemy selectable representing the query we want to perform

        result (QueryResult): The result of the query, according to the category
        result_count_ids (int): A simple count of the ids
        result_count_booking_codes: Distinct count of booking codes
        updated_at (str): Timestamp for the time the update method was ran
    """

    def __init__(self, query_category, id_range):
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
        now = get_current_timestamp()
        result = QueryResult(execute_query(self._engine, self._query))
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

    def get_engine(self) -> Engine:
        if self.query_category.startswith("mysql_"):
            engine = get_engine_mysql()
        elif self.query_category.startswith("bigquery_"):
            engine = get_engine_bigquery()
        else:
            raise ValueError(
                'query_category should be prefixed by "mysql_" or "bigquery_"'
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
        elif self.query_category == "bigquery_cnx":
            return self._query_bigquery_cnx()
        else:
            raise ValueError(f'Query category "{self.query_category}" is not valid.')

    def _query_mysql_trips(self) -> Select:
        """
        Get trip IDs and FH codes for booked trips in our MySQL database
        These are meant to be compared with the results of _query_bigquery_trips.

        Returns:
            query: A query to be executed with the appropriate sqlalchemy engine
        """

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
        """
        Get trip IDs and FH codes for all trips that have an active open request in our MySQL database.
        These are meant to be compared with the results of _query_bigquery_open; bookings missing in any of the two
        must be updated in BQ.

        Returns:
            query: A query to be executed with the appropriate sqlalchemy engine
        """

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
        """
        Get trip IDs and FH codes for all trips that have a cancellation request in our MySQL database and are *not*
        replaced.

        These are meant to be compared with the results of _query_bigquery_cnx.

        Returns:
            query: A query to be executed with the appropriate sqlalchemy engine
        """

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

    def _query_bigquery_cnx(self) -> Select:
        """
        Get trip IDs and FH codes for all trips that have a cancellation request in our BigQuery.

        These are meant to be compared with the results of _query_mysql_cnx.

        Returns:
            query: A query to be executed with the appropriate sqlalchemy engine
        """

        table_brt = get_table(
            self._engine, "booking_documents_production", "base_reporting_trips"
        )

        query = sa.select(
            table_brt.columns.trip_id_trip,
            table_brt.columns.booking_code,
        ).where(
              (table_brt.columns.trip_cancellation_request_id.is_not(None))
            & (table_brt.columns.trip_id_trip.between(*self.id_range))
        )

        return query


if __name__ == "__main__":
    id_min = 1354900
    range_ = (id_min, id_min + 100)

    handler = QueryHandler("mysql_cnx", range_)
    handler.update()
    print(handler.result)
    pass
