from pyspark.sql import DataFrame, SparkSession


class DatabaseInjector:
    def __init__(self, spark: SparkSession, config) -> None:
        self.spark: SparkSession = spark
        self.config = config

    def inject_data(self, data, schema, table_name) -> None:
        # Create DataFrame from data
        df: DataFrame = self.spark.createDataFrame(data, schema=schema)

        # Write DataFrame to the specified database table
        df.write.format("jdbc").options(
            url=self.config.MYSQL_PROPERTIES["url"],
            dbtable=table_name,
            user=self.config.MYSQL_PROPERTIES["user"],
            password=self.config.MYSQL_PROPERTIES["password"],
            driver=self.config.MYSQL_PROPERTIES["driver"],
        ).mode("overwrite").save()
