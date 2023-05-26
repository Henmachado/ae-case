from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import SparkSession


@dataclass
class ProductDailyEvents:
    """

    This class sits between our original source tables and the reporting environment. It
    transforms data from service tables into a standardized table that will help us to
    create more flexible and scalable data-marts

    """
    spark = SparkSession.builder.getOrCreate()

    table: DataFrame
    account_id_col: str
    amount_col: str
    requested_at_col: str
    completed_at_col: str
    status_col: str
    in_or_out_col: str
    product: str

    accounts: DataFrame = spark.table("accounts").select("account_id", "customer_id")
    customers: DataFrame = spark.table("customers").select("country_name", "customer_id")

    def get_transformed_table(self) -> DataFrame:
        """
        Transforms the input DataFrame into a standardized table.

        Returns:
            DataFrame: The product_daily_events transformed table.
        """
        return (
            self.table
            .select(
                F.col(self.account_id_col).alias("account_id"),
                F.col(self.amount_col).alias("amount"),
                F.col(self.requested_at_col).alias("txn_requested_at"),
                F.col(self.completed_at_col).alias("txn_completed_at"),
                F.col(self.status_col).alias("status"),
                F.when(
                    F.col(self.in_or_out_col).like("%_in%"), F.lit("in")
                ).otherwise(F.lit("out")).alias("in_or_out"),
                F.lit(self.product).alias("product"),
            )
            .join(self.accounts, on="account_id", how="left")
            .join(self.customers, on="customer_id", how="left")
            .withColumn("day", F.dayofmonth(F.from_unixtime("txn_requested_at")))
            .withColumn("month", F.date_format(F.from_unixtime("txn_requested_at"), "yyyy-MM"))
            .withColumn("event_date", F.date_format(F.from_unixtime("txn_requested_at"), "yyyy-MM-dd"))
            .drop("customer_id")
        )

    def write_in_spark_warehouse(self, mode) -> None:
        df = self.get_transformed_table()
        df.write.mode(mode).format("parquet").saveAsTable("products_daily_events")
