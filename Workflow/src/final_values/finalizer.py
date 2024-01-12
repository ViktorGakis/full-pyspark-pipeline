from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType


class FinalValues:
    def __init__(self, multipliers_df):
        self.multipliers_df = multipliers_df

    def final_values_cal(self, df):
        # Join the input DataFrame with the multipliers DataFrame
        df_with_multipliers = df.join(
            self.multipliers_df, on="INSTRUMENT_NAME", how="left"
        )

        # Calculate the final value
        return df_with_multipliers.withColumn(
            "FINAL_VALUE", F.col("VALUE") * F.coalesce(F.col("MULTIPLIER"), F.lit(1))
        )
