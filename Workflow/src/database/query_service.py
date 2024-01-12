class DatabaseService:
    def __init__(self, spark_session, config):
        self.spark_session = spark_session
        self.config = config

    def get_multipliers_df(self):
        multipliers_query = "SELECT NAME, MULTIPLIER FROM INSTRUMENT_PRICE_MODIFIER"
        return (
            self.spark_session.read.format("jdbc")
            .options(
                url=self.config.MYSQL_PROPERTIES["url"],
                dbtable=f"({multipliers_query}) as multipliers",
                properties=self.config.MYSQL_PROPERTIES,
            )
            .load()
        )
