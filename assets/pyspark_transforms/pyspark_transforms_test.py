from assets.pyspark_transforms.pyspark_transforms import pyspark_transforms, Input, Output


@pyspark_transforms(
    output_df=Output('test'),
    input_df1=Input('test'),
    input_df2=Input('test'),
)
def pyspark_training_test(sc, output_df, input_df1, input_df2):
    print('hey pyspark_training_test')


pyspark_training_test()
