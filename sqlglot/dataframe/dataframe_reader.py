import typing as t

from sqlglot import expressions as exp

if t.TYPE_CHECKING:
    from sqlglot.dataframe.dataframe import DataFrame
    from sqlglot.dataframe.session import SparkSession


class DataFrameReader:
    def __init__(self, spark: "SparkSession"):
        self.spark = spark

    def table(self, table_name: str) -> "DataFrame":
        from sqlglot.dataframe.dataframe import DataFrame

        return DataFrame(self.spark, exp.Select().from_(table_name).select("*"),
                         branch_id=self.spark.random_branch_id,
                         sequence_id=self.spark.random_sequence_id)._convert_leaf_to_cte()
