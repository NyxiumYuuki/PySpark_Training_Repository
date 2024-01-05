import os
import findspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.master("local[*]").getOrCreate()


sample_data = [
    {"name": "John    D.", "age": 30},
    {"name": "Alice   G.", "age": 25},
    {"name": "Bob  T.", "age": 35},
    {"name": "Eve   A.", "age": 28}
]
df = spark.createDataFrame(sample_data)





transformed_df = remove_extra_spaces(df, "name")
transformed_df.show()


def main():
    init_env()
    print("hey there")


if __name__ == "__main__":
    main()


def init_env():
    os.environ["JAVA_HOME"] = "C:\\Program Files\\Java\\jdk-11"
    os.environ["SPARK_HOME"] = "C:\\SPARK\\spark-3.1.1-bin-hadoop3.2"
    os.environ["HADOOP_HOME"] = "C:\\SPARK\\hadoop"

    findspark.init()