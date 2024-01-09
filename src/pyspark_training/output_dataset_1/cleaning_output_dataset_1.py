import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType


def remove_extra_spaces(df: DataFrame, column_name: str) -> DataFrame:
    """

    :param df:
    :param column_name:
    :return:
    """
    df_transformed = df.withColumn(
        column_name,
        F.regexp_replace(F.col(column_name), "\\s+", " ")
    )
    return df_transformed
