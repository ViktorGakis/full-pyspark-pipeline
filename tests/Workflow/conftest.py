# conftest.py
import os
import sys

from pytest import fixture

sys.path.append(os.path.dirname(__file__))

from Workflow.src.spark import Spark


@fixture(scope="module")
def spark_session(config):
    spark = Spark(config).create()

    yield spark

    spark.stop()
