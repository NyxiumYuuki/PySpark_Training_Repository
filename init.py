import os
import findspark
from pyspark.sql import SparkSession
from src.pyspark_training.output_dataset_1.compute_output_dataset_1 import compute_output_dataset_1


def init_env():
    os.environ["JAVA_HOME"] = "C:\\Program Files\\Java\\jdk-11"
    os.environ["SPARK_HOME"] = "C:\\SPARK\\spark-3.1.1-bin-hadoop3.2"
    os.environ["HADOOP_HOME"] = "C:\\SPARK\\hadoop"

    findspark.init()


def init_spark():
    return SparkSession.builder.master("local[*]").getOrCreate()


def main():
    print("hey there")
    init_env()
    spark_session = init_spark()

    compute_output_dataset_1(spark_session)


if __name__ == "__main__":
    main()
