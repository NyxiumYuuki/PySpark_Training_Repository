from dotenv import load_dotenv
import sys
import os
from pyspark.sql import SparkSession

load_dotenv()

print(os.environ["SPARK_HOME"])
print(os.environ["HADOOP_HOME"])
print(os.environ["JAVA_HOME"])
print("EXEC:")
print(sys.executable)

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame(
    [
        (1, "val1"),
        (2, "val2"),
        (3, "val3"),
        (4, "val4"),
    ]
)
df.show()

df.coalesce(1).write.mode("overwrite").csv("output/testoutput")
spark.stop()
print("Done\n")
