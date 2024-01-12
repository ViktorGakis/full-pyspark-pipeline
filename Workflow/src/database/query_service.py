class DatabaseService:
    def __init__(self, spark_session, config):
        self.spark_session = spark_session
        self.config = config

    def get_multipliers_df(self):
        return (
            self.spark_session.read.format("jdbc")
            .options(**self.config.MYSQL_PROPERTIES, dbtable=self.config.TABLE_NAME)
            .load()
            .withColumnRenamed("NAME", "INSTRUMENT_NAME")
        )
