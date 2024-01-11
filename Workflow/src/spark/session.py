# Workflow/src/spark/session.py
from typing import Optional

from ..config import Config  # If you choose to import Config directly


class Spark:
    def __init__(self, config: Config, appName: Optional[str] = None):
        self.config = config
        self.appName = appName if appName is not None else "SparkApplication"

    def create(self):
        import findspark

        findspark.init()
        findspark.add_jars(self.config.MYSQL_CONNECTOR_PATH)
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.appName(self.appName)
            .config("spark.jars", self.config.MYSQL_CONNECTOR_PATH)
            .config("spark.driver.extraClassPath", self.config.MYSQL_CONNECTOR_PATH)
            .config("spark.executor.extraClassPath", self.config.MYSQL_CONNECTOR_PATH)
            .getOrCreate()
        )
        return spark
