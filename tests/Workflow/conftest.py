# conftest.py
import os
import sys

from pytest import fixture

sys.path.append(os.path.dirname(__file__))

from pyspark.sql import SparkSession

from Workflow.src.spark import Spark


@fixture(scope="module")
def spark(config):
    spark: SparkSession = Spark(config).create()

    yield spark

    spark.stop()
