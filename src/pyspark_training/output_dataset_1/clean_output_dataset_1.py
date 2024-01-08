import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def clean_output_dataset_1(df: DataFrame) -> DataFrame:
    """

    :param df:
    :return:
    """
    df = remove_extra_spaces(df, 'name')

    return df


def remove_extra_spaces(df, column_name):
    df_transformed = df.withColumn(column_name, F.regexp_replace(F.col(column_name), "\\s+", " "))
    return df_transformed
