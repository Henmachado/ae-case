import json

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

from typing import List, Tuple

from investments.metrics.metrics import InvestmentsMetrics

spark = SparkSession.builder.getOrCreate()


# some utils functions in order to load the data

def read_csv(path: str) -> DataFrame:
    """loads csv data inferring the schema"""

    return (
        spark.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("treatEmptyValuesAsNulls", "true")
        .option("inferSchema", "true")
        .load(f"Tables/{path}")
    )


def create_catalog(tables: List) -> None:
    """creates a local spark-warehouse"""

    for table in tables:
        _df = read_csv(table)
        _df.createOrReplaceTempView(table)


def read_json_from_file(file_path: str) -> List:
    """reads the raw json"""

    with open(file_path, 'r') as f:
        json_data = f.read()
        data = json.loads(json_data)
    return data


def write_json_to_file(data: List[str], file_path) -> None:
    """write the json back as one object per line"""

    with open(file_path, 'w') as file:
        for item in data:
            json.dump(item, file)
            file.write('\n')


def read_json_output(output_file: str, schema: str) -> DataFrame:
    """reads the processed json on spark"""

    return spark.read.schema(schema).json(output_file)


def unnest_json_df(df: DataFrame) -> DataFrame:
    """unnest the raw data into columns"""

    return (
        df.select("account_id", F.explode("transactions").alias("transaction"))
        .select(
            F.col("transaction.transaction_id").cast('long').alias("id"),
            F.col("account_id").cast('long').alias("account_id"),
            F.col("transaction.amount").cast('double').alias("amount"),
            F.col("transaction.investment_requested_at").cast('int').alias("investment_requested_at"),
            F.col("transaction.investment_completed_at").cast('string').alias("investment_completed_at"),
            F.col("transaction.status").cast('string').alias("status"),
            F.col("transaction.type").cast('string').alias("type"),
        )
    )


def create_investments_df() -> None:
    """data pipeline that consumes the raw json and write it in the catalog"""

    input_file_path = '../../Tables/investments/investments_json.txt'
    output_file_path = '../../Tables/investments/investments.json'

    schema = """
        account_id STRING,
        transactions ARRAY<STRUCT<
            transaction_id: STRING,
            status: STRING,
            amount: STRING,
            investment_requested_at: STRING,
            investment_completed_at: STRING,
            investment_completed_at_timestamp: DATE,
            type: STRING
        >>
    """

    json_data = read_json_from_file(input_file_path)
    write_json_to_file(json_data, output_file_path)

    json_df = read_json_output(output_file_path, schema)
    unnested_df = unnest_json_df(json_df)

    return unnested_df.createOrReplaceTempView("investments")


def create_investments_daily_balance_df(
        df: DataFrame,
        interval: Tuple[str, str],
        metrics: InvestmentsMetrics = InvestmentsMetrics(),
) -> DataFrame:
    """ Creates the Investments Daily Balance Report
    :rtype: object
    """

    return (
        df
        .where(F.col("event_date").between(*interval))
        .where(F.col("status") == "completed")
        .where(F.col("product") == "investments")
        .groupBy("event_date", "day", "month", "account_id")
        .agg(*metrics.get_all())
        .drop("event_date", "balance", "previous_day_balance", "movements")
    )
