import os
import sys
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
import inspect
from assets.pipegraph.pipegraph import PipeGraph


###############################################################
@PipeGraph
def compute_dataset_1(df1: DataFrame, df2: DataFrame) -> DataFrame:
    """
    Compute the dataset_1
    :param df1:
    :param df2:
    :return:
    """
    cleaned_df1 = clean_df1(df1)
    cleaned_df2 = clean_df2(df2)

    df = join_df1_df2(cleaned_df1, cleaned_df2, 'id', how='left')

    df = add_letter_column(df)
    df = add_calculated_column(df)

    return df


def clean_df1(df1: DataFrame) -> DataFrame:
    """
    Clean the dataframe
    :param df1:
    :return:
    """
    df1 = clean_df1_space(df1)

    return df1


@PipeGraph
def clean_df1_space(df1: DataFrame) -> DataFrame:
    """
    Clean space of dataframe
    :param df1:
    :return:
    """
    # clean space
    return df1


def clean_df2(df2: DataFrame) -> DataFrame:
    """
    Clean the dataframe
    :param df2:
    :return:
    """
    df2 = clean_df2_space(df2)
    df2 = clean_df2_letter(df2)
    return df2


@PipeGraph
def clean_df2_space(df2: DataFrame) -> DataFrame:
    """
    Clean space of dataframe
    :param df2:
    :return:
    """
    # clean space
    return df2


@PipeGraph
def clean_df2_letter(df2: DataFrame) -> DataFrame:
    """
    Clean the letter of dataframe
    :param df2:
    :return:
    """
    # clean letter
    return df2


@PipeGraph
def add_letter_column(df: DataFrame) -> DataFrame:
    """
    Adds a letter column to dataframe
    :param df:
    :return:
    """
    # Add column letter
    return df


@PipeGraph
def add_calculated_column(df: DataFrame) -> DataFrame:
    """
    Adds calculated column to dataframe
    :param df:
    :return:
    """
    # Add calculated column
    return df


###############################################################
@PipeGraph
def compute_dataset_2(df2: DataFrame) -> DataFrame:
    """
    Compute the dataset_2
    :param df2:
    :return:
    """
    cleaned_df2 = clean_df2(df2)

    df = add_letter_column(cleaned_df2)
    df = add_complex_calculated_column(df)

    return df


@PipeGraph
def add_complex_calculated_column(df: DataFrame) -> DataFrame:
    """
    Compute the complex_calculated_column
    :param df:
    :return:
    """
    # Add complex calculated column
    return df


@PipeGraph
def join_df1_df2(df1: DataFrame, df2: DataFrame, on: str, how='left') -> DataFrame:
    """
    Join two dataframes
    :param df1:
    :param df2:
    :param on:
    :param how:
    :return:
    """
    return df1.join(df2, on, how)


###############################################################
def init_spark():
    return SparkSession.builder.master("local[*]").getOrCreate()


def main():
    load_dotenv()
    print(os.environ["SPARK_HOME"])  # spark-3.5.0-bin-hadoop3
    print(os.environ["HADOOP_HOME"])  # spark-3.5.0-bin-hadoop3, + winutils et dll hadoop 3.0
    print(os.environ["JAVA_HOME"])  # java 8 local (zulu)
    print("EXEC:")
    print(sys.executable)
    spark_session = init_spark()

    PipeGraph.json()

    df1 = spark_session.createDataFrame(
        [(1, 'name 1'), (2, 'name 2'), (3, 'name 3')],
        StructType([
            StructField('id', IntegerType()),
            StructField('name', StringType()),
        ])
    )
    df2 = spark_session.createDataFrame(
        [(1, 'adult'), (2, 'child'), (3, 'teenager')],
        StructType([
            StructField('id', IntegerType()),
            StructField('life_stage', StringType()),
        ])
    )

    output_dataset_1 = compute_dataset_1(df1, df2)
    output_dataset_2 = compute_dataset_2(df2)

    output_dataset_1.show()
    output_dataset_2.show()
    spark_session.stop()

    print(f'PipeGraph JSON id:{PipeGraph.get_node_id()}')
    PipeGraph.json()


if __name__ == "__main__":
    main()
