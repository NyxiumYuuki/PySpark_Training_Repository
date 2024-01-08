import os
import sys
import findspark
import logging
import pytest
from pyspark.sql import SparkSession


@pytest.fixture
def spark_session(request):
    """
    Return a Spark Session
    :param request:
    :return: Spark session
    """
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    os.environ["JAVA_HOME"] = "C:\\Program Files\\Java\\jdk-11"
    os.environ["SPARK_HOME"] = "C:\\SPARK\\spark-3.1.1-bin-hadoop3.2"
    os.environ["HADOOP_HOME"] = "C:\\SPARK\\hadoop"

    findspark.init()

    spark = SparkSession.builder.master("local[*]").getOrCreate()
    request.addfinalizer(lambda: spark.stop())
    quiet_py4j()
    return spark


def quiet_py4j():
    """Suppress spark logging for the test context."""
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)
