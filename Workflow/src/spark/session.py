# Workflow/src/spark/session.py
from typing import Optional

import findspark

from ..config import Config

findspark.init()
findspark.add_jars(Config.MYSQL_CONNECTOR_PATH)
from pyspark.sql import SparkSession


class Spark:
    def __init__(self, appName: Optional[str] = "SparkApplication"):
        self.appName: str | None = appName

    def create(self):
        spark = (
            SparkSession.builder.appName(self.appName)
            .config("spark.jars", Config.MYSQL_CONNECTOR_PATH)
            .config("spark.driver.extraClassPath", Config.MYSQL_CONNECTOR_PATH)
            .config("spark.executor.extraClassPath", Config.MYSQL_CONNECTOR_PATH)
            .getOrCreate()
        )
        return spark
