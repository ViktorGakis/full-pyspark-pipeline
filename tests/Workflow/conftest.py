# conftest.py
import os
import sys

from pytest import fixture

sys.path.append(os.path.dirname(__file__))

from pyspark.sql import DataFrame, SparkSession

from Workflow.src.data_loading import LoadTxtData, TxtSchemaProvider
from Workflow.src.spark import Spark


@fixture(scope="module")
def spark(config):
    spark: SparkSession = Spark(config).create()

    yield spark

    spark.stop()


@fixture(scope="module")
def df_txt(spark, config) -> DataFrame:
    return LoadTxtData(
        spark, TxtSchemaProvider.schema, config.TXT_FILE_REL_PATH_STR  # type: ignore
    ).load_source_file()
