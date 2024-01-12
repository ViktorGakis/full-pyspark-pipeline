from random import randint
from time import sleep


class FinalValues:
    def __init__(self, rows, db_query_function):
        """
        rows: List of rows, each row is a dictionary representing data read from a .txt file.
        db_query_function: Function obtained from the closure query_db_closure.
        """
        self.rows = rows
        self.db_query_function = db_query_function

    def final_value_calc_row(self, row):
        """
        Calculate the final value for a given row.
        row: Dictionary representing a single row of data.
        """
        instrument = row["INSTRUMENT_NAME"]
        result_df = self.db_query_function(instrument)
        old_value = row["VALUE"]

        if result_df.isEmpty():
            return old_value
        return old_value * result_df.collect()[0]["MULTIPLIER"]

    def final_values_cal(self, timelag: bool = True):
        """
        Calculate final values for all rows.
        """
        final_values = []
        for row in self.rows:
            final_value = self.final_value_calc_row(row)
            final_values.append(final_value)
            if timelag:
                sleep(randint(1, 10))
        return final_values
