from typing import NamedTuple, List

from pyspark.sql import Column as PysparkColumn
from pyspark.sql import functions as F
from pyspark.sql import Window


class CustomDimensions(NamedTuple):
    """
    Defines all custom dimensions that can be added to the 'products_daily_events' table in
    order to create our business metrics.
    """

    WINDOW_ACCOUNT = (
        Window()
        .partitionBy("account_id")
        .orderBy(F.col("txn_requested_at"))
    )

    WINDOW_ACCOUNT_24H = (
        Window()
        .partitionBy("account_id")
        .orderBy(F.col("txn_requested_at").cast("long"))
        .rangeBetween(-24, Window.currentRow)
    )

    WINDOW_ACCOUNT_EVER = (
        Window()
        .partitionBy("account_id")
        .orderBy(F.col("txn_requested_at"))
        .rangeBetween(Window.unboundedPreceding, Window.currentRow)
    )

    TXN_REQUESTED_AT_TS = (
        F.from_unixtime("txn_requested_at")
    ).alias("txn_requested_at_ts")

    TXN_COMPLETED_AT_TS = (
        F.from_unixtime("txn_completed_at")
    ).alias("txn_completed_at_ts")

    TIME_TO_COMPLETE_IN_SECONDS = (
            F.col("txn_completed_at").cast("long") - F.col("txn_requested_at").cast("long")
    ).alias("time_to_complete_in_seconds")

    COUNT_TXNS_PER_ACCOUNT_EVER = (
        F.sum(F.lit(1)).over(WINDOW_ACCOUNT_EVER)
    ).alias("count_txns_orders_per_account_ever")

    LAST_TXN_DATE = (
        F.lag("txn_requested_at", 1).over(WINDOW_ACCOUNT)
    ).alias("last_txn_date")

    TIME_FROM_LAST_TXN_IN_DAYS = (
        ((F.col("txn_completed_at").cast("long") - LAST_TXN_DATE.cast("long")) / 86400)
    ).alias("time_from_last_txn_in_days")

    @classmethod
    def get_all(cls) -> List[F.Column]:
        """
        The get_all() class method is an option to generate all at once in a single list
        """
        return [
            cls.TXN_REQUESTED_AT_TS,
            cls.TXN_COMPLETED_AT_TS,
            cls.TIME_TO_COMPLETE_IN_SECONDS,
            cls.COUNT_TXNS_PER_ACCOUNT_EVER,
            cls.LAST_TXN_DATE,
            cls.TIME_FROM_LAST_TXN_IN_DAYS,
        ]


class Metrics(NamedTuple):
    """
    Defines all logics and businesses rules for our metrics.
    """

    FAILED = (F.col("status") == "failed")

    COMPLETED = (F.col("status") == "completed")

    FIRST_TXN = (F.col("count_txns_orders_per_account_ever") == 1)

    TOTAL_AMOUNT = F.sum(F.col("amount")).alias("total_amount")

    TOTAL_AMOUNT_COMPLETED = (
        F.sum(F.when(COMPLETED, F.col("amount")).otherwise(0))
    ).alias("total_amount_completed")

    TOTAL_AMOUNT_FAILED = (
        F.sum(F.when(FAILED, F.col("amount")).otherwise(0))
    ).alias("total_amount_failed")

    TOTAL_TRANSFER_IN = (
        F.sum(F.when(F.col("in_or_out") == "in", F.col("amount")).otherwise(0))
    ).alias("total_transfer_in")

    TOTAL_TRANSFER_OUT = (
        F.sum(F.when(F.col("in_or_out") == "out", F.col("amount")).otherwise(0))
    ).alias("total_transfer_out")

    ACCOUNT_MONTHLY_BALANCE = (
            TOTAL_TRANSFER_IN - TOTAL_TRANSFER_OUT
    ).alias("account_monthly_balance")

    FAIL_RATE = F.round(
        TOTAL_AMOUNT_FAILED / TOTAL_AMOUNT_COMPLETED, 2
    ).alias("fail_rate")

    COMPLETED_RATE = F.round(
        TOTAL_AMOUNT_COMPLETED / TOTAL_AMOUNT, 2
    ).alias("completed_rate")

    AVG_TIME_TO_COMPLETED = (
        F.avg(F.col("time_to_complete_in_seconds"))
    ).alias("avg_time_to_completed")

    AVG_RECENCY = (
        F.avg(F.col("time_from_last_txn_in_days"))
    ).alias("avg_recency")

    TOTAL_ORDERS = (
        F.sum(F.lit(1))
    ).alias("total_orders")

    TOTAL_FIRST_TXN_ORDERS = (
        F.sum(F.when(FIRST_TXN, F.lit(1)).otherwise(0))
    ).alias("total_first_txn_orders")

    ATV = (
        F.avg(F.col("amount"))
    ).alias("atv")

    P99_TIME_TO_COMPLETED = (
        F.percentile_approx("time_to_complete_in_seconds", 0.99).alias("p99")
    )

    P90_TIME_TO_COMPLETED = (
        F.percentile_approx("time_to_complete_in_seconds", 0.90).alias("p90")
    )

    @classmethod
    def get_all(cls) -> List[PysparkColumn]:
        """
        The get_all() class method is an option to generate all at once in a single list
        """
        return [
            cls.TOTAL_AMOUNT,
            cls.TOTAL_AMOUNT_COMPLETED,
            cls.TOTAL_AMOUNT_FAILED,
            cls.FAIL_RATE,
            cls.COMPLETED_RATE,
            cls.AVG_TIME_TO_COMPLETED,
            cls.AVG_RECENCY,
            cls.TOTAL_ORDERS,
            cls.TOTAL_FIRST_TXN_ORDERS,
            cls.ATV,
            cls.P99_TIME_TO_COMPLETED,
            cls.P90_TIME_TO_COMPLETED,
        ]

