from account_monthly_balance.etl.account_monthly_balance import create_account_monthly_balance

from pyspark.sql import functions as F
from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder.getOrCreate()

    df_products_daily_events = (
        spark.table("products_daily_events")
    )

    create_account_monthly_balance(
        df=df_products_daily_events,
        interval=("2020-01-01", "2021-01-01"),
        products=["non_pix", "pix"]
    )


if __name__ == "__main__":
    main()
