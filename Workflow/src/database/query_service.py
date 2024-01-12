# Workflow/src/database/query_service.py
from datetime import datetime, timezone


class DatabaseQueryService:
    def __init__(self, spark_session, table_name, properties, schema, min_update_time):
        self.spark = spark_session
        self.table_name = table_name
        self.properties = properties
        self.schema = schema
        self.min_update_time = min_update_time

    def handle_query(self, instrument_name):
        """Function to query a specific instrument in the database."""
        query = f"SELECT * FROM {self.table_name} WHERE NAME = '{instrument_name}'"
        return (
            self.spark.read.format("jdbc")
            .options(**self.properties, query=query)
            .load()
        )

    def query_db_closure(self, verbose=False):
        result_df = self.spark.createDataFrame([], schema=self.schema)
        last_time = datetime.now(timezone.utc)
        last_instrument_name = ""
        call_counter = 1

        def query_db(instrument_name):
            nonlocal result_df, last_time, last_instrument_name, call_counter

            current_time = datetime.now(timezone.utc)
            timediff = (current_time - last_time).seconds

            if verbose:
                print("----query_db----")
                print(
                    f"{last_instrument_name=}, current_instrument_name={instrument_name}"
                )
                print(f"{call_counter=}")

            if (
                timediff <= self.min_update_time
                and last_instrument_name == instrument_name
            ):
                if verbose:
                    current_time_str = datetime.strftime(
                        current_time, "%d/%m/%Y, %H:%M:%S"
                    )
                    print(f"SAME Q {current_time_str}, {timediff=}")
                    print(" ")
                last_time = current_time
                last_instrument_name = instrument_name
                call_counter += 1
                return result_df

            if verbose:
                current_time_str = datetime.strftime(current_time, "%d/%m/%Y, %H:%M:%S")
                print(f"NEW Q {current_time_str}, {timediff=}")
                print(" ")

            result_df = self.handle_query(instrument_name)
            last_time = current_time
            last_instrument_name = instrument_name
            call_counter += 1
            return result_df

        return query_db
