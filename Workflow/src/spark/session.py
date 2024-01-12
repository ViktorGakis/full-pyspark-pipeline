from typing import Any

from pyspark.sql import SparkSession


class Spark:
    def __init__(self, config: Any) -> None:
        self.config = config

    def create(self) -> SparkSession:
        import findspark

        findspark.init()
        findspark.add_jars(self.config.MYSQL_CONNECTOR_PATH)
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.appName(self.config.appName)
            .config("spark.jars", self.config.MYSQL_CONNECTOR_PATH)
            .config("spark.driver.extraClassPath", self.config.MYSQL_CONNECTOR_PATH)
            .config("spark.executor.extraClassPath", self.config.MYSQL_CONNECTOR_PATH)
            .getOrCreate()
        )
        return spark
