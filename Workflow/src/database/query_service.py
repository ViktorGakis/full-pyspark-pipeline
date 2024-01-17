from typing import Optional


class DatabaseService:
    def __init__(self, spark_session, config):
        self.spark_session = spark_session
        self.config = config

    def get_multipliers_df(self, table: Optional[str] = None):
        if not table:
            table = self.config.TABLE_NAME
        return (
            self.spark_session.read.format("jdbc")
            .options(**self.config.MYSQL_PROPERTIES, dbtable=table)
            .load()
            .withColumnRenamed("NAME", "INSTRUMENT_NAME")
        )
