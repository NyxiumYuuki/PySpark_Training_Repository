from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

import src.pyspark_training.output_dataset_1.cleaning_output_dataset_1 as C
import src.pyspark_training.output_dataset_1.processing_output_dataset_1 as P

INPUT_DATASET_1_PATH = './assets/output_dataset_1/raw/RAW_input_output_dataset_1.csv'
OUTPUT_DATASET_1_PATH = './assets/output_dataset_1/output/OUTPUT_output_dataset_1.csv'


def compute_output_dataset_1(spark_session: SparkSession):
    """
    Compute the output of output_dataset_1
    :param spark_session:
    :return:
    """
    df_schema = StructType([
        StructField('name', StringType(), False),
        StructField('age', IntegerType(), False)
    ])
    df = spark_session.read.csv(INPUT_DATASET_1_PATH, header=True, schema=df_schema)

    # Cleaning
    cleaned_df = C.remove_extra_spaces(df, 'name')

    # Processing
    df = P.add_life_stage(cleaned_df)

    df.show()
    df.write.mode('overwrite').csv(OUTPUT_DATASET_1_PATH)
