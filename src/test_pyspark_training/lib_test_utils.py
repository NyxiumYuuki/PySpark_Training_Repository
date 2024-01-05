
def assert_df_equal(df1, df2):

    try:
        assert df1.schema() == df2.schema()
    except AssertionError:
        print('Error Schema')
        print(df1.schema())
        print(df1.schema())

    try:
        assert df1.equals(df2)
    except AssertionError:
        print('Error Schema')
        df1.show()
        df2.show()
