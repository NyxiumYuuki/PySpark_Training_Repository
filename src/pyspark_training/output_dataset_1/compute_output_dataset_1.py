import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from src.pyspark_training.output_dataset_1.clean_output_dataset_1 import clean_output_dataset_1


def compute_output_dataset_1(df: DataFrame) -> DataFrame:

    df = clean_output_dataset_1(df)

    df = add_life_stage(df)

    return df


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
        .otherwise(F.lit('adult'))
    )

    return df
