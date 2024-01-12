class DatabaseQueryService:
    def __init__(self, spark_session, table_name, properties):
        self.spark = spark_session
        self.table_name = table_name
        self.properties = properties

    def handle_query(self, instrument_name):
        """Function to query a specific instrument in the database."""
        query: str = f"SELECT * FROM {self.table_name} WHERE NAME = '{instrument_name}'"
        return (
            self.spark.read.format("jdbc")
            .options(**self.properties, query=query)
            .load()
        )
