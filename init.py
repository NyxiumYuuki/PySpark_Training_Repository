import os
import sys
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from src.pyspark_training.output_dataset_1.compute_output_dataset_1 import compute_output_dataset_1


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

    compute_output_dataset_1(spark_session)


if __name__ == "__main__":
    main()
