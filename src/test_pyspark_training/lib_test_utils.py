from pyspark.sql import DataFrame
from pyspark_test import assert_pyspark_df_equal


def assert_df_equal(actual_df: DataFrame, expected_df: DataFrame) -> None:

    try:
        assert actual_df.schema == expected_df.schema
    except AssertionError:
        print('Error Schema')
        print('Actual :\n')
        actual_df.printSchema()
        print('Expected :\n')
        expected_df.printSchema()

    assert_pyspark_df_equal(actual_df, expected_df)
