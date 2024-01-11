# Workflow/src/database/query_service.py
class DatabaseQueryService:
    def handle_query(self, spark_session, query):
        """Function to query a specific instrument in the database."""
        return spark_session.sql(query)

    def query_db_closure(self, spark_session):
        """Generates a closure function for querying the database."""

        def query(query):
            return self.handle_query(spark_session, query)

        return query
