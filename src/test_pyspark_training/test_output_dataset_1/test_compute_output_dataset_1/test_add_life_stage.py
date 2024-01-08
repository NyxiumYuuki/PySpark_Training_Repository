from pyspark.sql import types as T
from src.test_pyspark_training.lib_test_utils import assert_df_equal
from src.pyspark_training.output_dataset_1.compute_output_dataset_1 import add_life_stage


def test_add_life_stage(spark_session):

    input_schema = T.StructType(
        [
            T.StructField('name', T.StringType(), False),
            T.StructField('age', T.IntegerType(), False),
        ]
    )
    input_data = [
        ('Alice G.', 13),
        ('John B.', 20),
        ('Jack W.', 19),
        ('Bob T.', 35),
        ('John D.', 9),
        ('Eve A.', 12),
    ]
    input_df = spark_session.createDataFrame(input_data, input_schema)

    expected_schema = T.StructType(
        [
            T.StructField('name', T.StringType(), False),
            T.StructField('age', T.IntegerType(), False),
            T.StructField('life_stage', T.StringType(), False),
        ]
    )
    expected_data = [
        ('Alice G.', 13, 'teenager'),
        ('John B.', 20, 'adult'),
        ('Jack W.', 19, 'teenager'),
        ('Bob T.', 35, 'adult'),
        ('John D.', 9, 'child'),
        ('Eve A.', 12, 'child'),
    ]
    expected_df = spark_session.createDataFrame(expected_data, expected_schema)

    df = add_life_stage(input_df)

    assert_df_equal(df, expected_df)
