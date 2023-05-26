from investments.etl.investments_daily_balance import create_investments_daily_balance_df

from pyspark.sql import functions as F
from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder.getOrCreate()

    investments_users = (
        spark.table("investment_accounts_to_send")
        .toPandas()
        .account_id.tolist()
    )

    df_products_daily_events = (
        spark.table("products_daily_events")
        .where(F.col("account_id").isin(investments_users))
    )

    create_investments_daily_balance_df(
        df=df_products_daily_events,
        interval=("2020-01-01", "2021-01-01"),
    )


if __name__ == "__main__":
    main()
