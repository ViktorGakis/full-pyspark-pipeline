from pyspark.sql import SparkSession


class DatabaseInjector:
    def __init__(self, spark: SparkSession, config):
        self.spark: SparkSession = spark
        self.config = config

    def inject_data(self, data, schema, table_name):
        # Create DataFrame from data
        df = self.spark.createDataFrame(data, schema=schema)

        # Write DataFrame to the specified database table
        df.write.format("jdbc").options(
            url=self.config.MYSQL_PROPERTIES["url"],
            dbtable=table_name,
            user=self.config.MYSQL_PROPERTIES["user"],
            password=self.config.MYSQL_PROPERTIES["password"],
            driver=self.config.MYSQL_PROPERTIES["driver"],
        ).mode("overwrite").save()
