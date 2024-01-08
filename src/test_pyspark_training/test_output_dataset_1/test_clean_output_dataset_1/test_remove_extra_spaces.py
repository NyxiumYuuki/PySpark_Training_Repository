from pyspark.sql import types as T
from src.test_pyspark_training.lib_test_utils import assert_df_equal
from src.pyspark_training.output_dataset_1.clean_output_dataset_1 import remove_extra_spaces


def test_remove_extra_spaces(spark_session):

    input_schema = T.StructType(
        [
            T.StructField('name', T.StringType(), False),
            T.StructField('age', T.IntegerType(), False),
        ]
    )
    input_data = [
        ('John    D.',  30),
        ('Alice   G.',  25),
        ('Bob  T.',     35),
        ('Eve   A.',    28),
    ]
    input_df = spark_session.createDataFrame(input_data, input_schema)

    expected_schema = T.StructType(
        [
            T.StructField('name', T.StringType(), False),
            T.StructField('age', T.IntegerType(), False),
        ]
    )
    expected_data = [
        ('John D.', 30),
        ('Alice G.', 25),
        ('Bob T.', 35),
        ('Eve A.', 28),
    ]
    expected_df = spark_session.createDataFrame(expected_data, expected_schema)

    df = remove_extra_spaces(input_df, 'name')

    assert_df_equal(df, expected_df)
