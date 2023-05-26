from typing import Tuple, List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from account_monthly_balance.metrics.metrics import Metrics


def create_account_monthly_balance(
        df: DataFrame,
        interval: Tuple[str, str],
        products: List[str],
        metrics: Metrics = Metrics(),
) -> DataFrame:
    """ Creates the Account Monthly Balance Report """

    return (
        df
        .where(F.col("event_date").between(*interval))
        .where(F.col("status") == "completed")
        .where(F.col("product").isin(products))
        .groupBy("month", "account_id")
        .agg(metrics.TOTAL_TRANSFER_IN,
             metrics.TOTAL_TRANSFER_OUT,
             metrics.ACCOUNT_MONTHLY_BALANCE,
             )
    )
