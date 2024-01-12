from datetime import datetime, timedelta

from pyspark.sql import SparkSession


class FinalValues:
    def __init__(self, db_query_function, cache_expiry=5):
        self.db_query_function = db_query_function
        self.cache = {}  # Cache to store multipliers
        self.cache_expiry = cache_expiry  # Cache expiry time in seconds

    def get_multiplier(self, instrument):
        current_time = datetime.now()
        if (
            instrument in self.cache
            and (current_time - self.cache[instrument]["timestamp"]).seconds
            < self.cache_expiry
        ):
            return self.cache[instrument]["multiplier"]

        result_df = self.db_query_function(instrument)
        multiplier = (
            result_df.collect()[0]["MULTIPLIER"] if not result_df.isEmpty() else None
        )
        self.cache[instrument] = {"multiplier": multiplier, "timestamp": current_time}
        return multiplier

    def final_value_calc_row(self, row):
        multiplier = self.get_multiplier(row["INSTRUMENT_NAME"])
        return row["VALUE"] * multiplier if multiplier is not None else row["VALUE"]

    def final_values_cal(self, spark: SparkSession):
        df = spark.createDataFrame(self.rows)
        udf_calculate_final_value = udf(self.final_value_calc_row)
        return df.withColumn(
            "FINAL_VALUE", udf_calculate_final_value(df["INSTRUMENT_NAME"], df["VALUE"])
        )
