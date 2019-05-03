

import logging

import pytest
from pyspark.sql import SparkSession


def quiet_py4j():
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark_session(request):
    spark = (SparkSession
             .builder
             .master("local[2]")
             .appName("pytest local testing")
             # .enableHiveSupport()
             .getOrCreate())
    request.addfinalizer(lambda: spark.stop())

    quiet_py4j()
    return spark