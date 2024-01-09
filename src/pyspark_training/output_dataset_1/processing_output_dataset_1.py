import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def add_life_stage(df: DataFrame) -> DataFrame:
    """
    Add life stage
        child if age < 13
        teenager if age >= 13 and <= 19
        adult for age>20
    :param df:
    :return:
    """
    df = df.withColumn(
        'life_stage',
        F.when(F.col('age') < 13, F.lit('child'))
        .when(F.col('age').between(13, 19), F.lit('teenager'))
        .when(F.col('age') >= 20, F.lit('adult'))
    )

    return df


def join_with_broadcast(big_df: DataFrame, smaller_df: DataFrame) -> DataFrame:
    """
    Join big dataset and smaller dataset with broadcast
    :param big_df:
    :param smaller_df:
    :return:
    """
    df = big_df.join(
        F.broadcast(smaller_df)
    )

    return df
