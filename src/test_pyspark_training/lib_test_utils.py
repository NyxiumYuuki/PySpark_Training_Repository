from pyspark_test import assert_pyspark_df_equal


def assert_df_equal(df1, df2):

    try:
        assert df1.schema == df2.schema
    except AssertionError:
        print('Error Schema')
        print('df1\n')
        df1.printSchema()
        print('df2\n')
        df2.printSchema()

    assert_pyspark_df_equal(df1, df2)
