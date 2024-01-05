import os
import findspark
from pyspark.sql import SparkSession


def init_env():
    os.environ["JAVA_HOME"] = "C:\\Program Files\\Java\\jdk-11"
    os.environ["SPARK_HOME"] = "C:\\SPARK\\spark-3.1.1-bin-hadoop3.2"
    os.environ["HADOOP_HOME"] = "C:\\SPARK\\hadoop"

    findspark.init()


def init_spark():
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    df = spark.createDataFrame([
        {'name': 'OUI OUI', 'age': 30},
    ])
    df.show()


def main():
    print("hey there")
    init_env()
    init_spark()


if __name__ == "__main__":
    main()
