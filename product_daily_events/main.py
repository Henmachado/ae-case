from product_daily_events.config.config import ProductDailyEvents

from pyspark.sql import functions as F
from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder.getOrCreate()

    pix_movements = spark.table("pix_movements")
    transfer_ins = spark.table("transfer_ins").withColumn("in_or_out", F.lit("non_pix_in"))
    transfer_outs = spark.table("transfer_outs").withColumn("in_or_out", F.lit("non_pix_out"))
    investments = spark.table("investments")

    daily_events_pix = ProductDailyEvents(
        table=pix_movements,
        account_id_col="account_id",
        amount_col="pix_amount",
        requested_at_col="pix_requested_at",
        completed_at_col="pix_completed_at",
        status_col="status",
        in_or_out_col="in_or_out",
        product="pix"
    )

    daily_events_pix.write_in_spark_warehouse(mode="overwrite")

    daily_events_transfer_in = ProductDailyEvents(
        table=transfer_ins,
        account_id_col="account_id",
        amount_col="amount",
        requested_at_col="transaction_requested_at",
        completed_at_col="transaction_completed_at",
        status_col="status",
        in_or_out_col="in_or_out",
        product="non_pix",
    )

    daily_events_transfer_in.write_in_spark_warehouse(mode="append")

    daily_events_transfer_out = ProductDailyEvents(
        table=transfer_outs,
        account_id_col="account_id",
        amount_col="amount",
        requested_at_col="transaction_requested_at",
        completed_at_col="transaction_completed_at",
        status_col="status",
        in_or_out_col="in_or_out",
        product="non_pix",
    )

    daily_events_transfer_out.write_in_spark_warehouse(mode="append")

    daily_events_investments = ProductDailyEvents(
        table=investments,
        account_id_col="account_id",
        amount_col="amount",
        requested_at_col="investment_requested_at",
        completed_at_col="investment_completed_at",
        status_col="status",
        in_or_out_col="type",
        product="investments",
    )

    daily_events_investments.write_in_spark_warehouse(mode="append")


if __name__ == "__main__":
    main()
