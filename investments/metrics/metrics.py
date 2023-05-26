from pyspark.sql import Column as PysparkColumn
from pyspark.sql import functions as F
from pyspark.sql import Window

from typing import NamedTuple, List


class InvestmentsMetrics(NamedTuple):
    """
    Defines all logics, businesses rules and constants for our investments metrics
    """

    INVESTMENTS_LAG_WINDOW = (
        Window()
        .partitionBy("account_id")
        .orderBy(F.col("event_date"))
    )

    INVESTMENTS_CUM_WINDOW = (
        Window()
        .partitionBy("account_id")
        .orderBy(F.col("event_date"))
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    FIXED_RATE = 0.0001

    TOTAL_DEPOSIT = (
        F.sum(F.when(F.col("in_or_out") == "in", F.col("amount")).otherwise(0))
    ).alias("deposit")

    TOTAL_WITHDRAWAL = (
        F.sum(F.when(F.col("in_or_out") == "out", F.col("amount")).otherwise(0))
    ).alias("withdrawal")

    TOTAL_BALANCE = (
            TOTAL_DEPOSIT - TOTAL_WITHDRAWAL
    ).alias("balance")

    PREVIOUS_DAY_TOTAL_BALANCE = (
        F.lag(TOTAL_BALANCE, default=0).over(INVESTMENTS_LAG_WINDOW)
    ).alias("previous_day_balance")

    TOTAL_MOVEMENTS = (
        F.sum(TOTAL_BALANCE).over(INVESTMENTS_CUM_WINDOW)
    ).alias("movements")

    _END_OF_DAY_INCOME = (
            TOTAL_MOVEMENTS * F.lit(FIXED_RATE)
    ).alias("_end_of_day_income")

    END_OF_DAY_INCOME = (
        F.when(_END_OF_DAY_INCOME < 0, F.lit(0)).otherwise(_END_OF_DAY_INCOME)
    ).alias("end_of_day_income")

    ACCOUNT_DAILY_BALANCE = (
            TOTAL_MOVEMENTS + END_OF_DAY_INCOME
    ).alias("account_daily_balance")

    @classmethod
    def get_all(cls) -> List[PysparkColumn]:
        """
        The get_all() class method is an option to generate all at once in a single list
        """
        return [
            cls.TOTAL_DEPOSIT,
            cls.TOTAL_WITHDRAWAL,
            cls.TOTAL_BALANCE,
            cls.PREVIOUS_DAY_TOTAL_BALANCE,
            cls.TOTAL_MOVEMENTS,
            cls.END_OF_DAY_INCOME,
            cls.ACCOUNT_DAILY_BALANCE,
        ]