from pyspark.sql.functions import col, day, mean, month, year


class CalculationEngine:
    """Class for performing various calculations on financial instruments."""

    @staticmethod
    def instr_1_mean(df):
        df_local = df
        """Calculate the mean for INSTRUMENT1."""
        print("mean for INSTRUMENT1.")
        (
            df_local.groupBy("INSTRUMENT_NAME")
            .agg(mean("VALUE").alias("MEAN"))
            .filter(col("INSTRUMENT_NAME") == "INSTRUMENT1")
        ).show()

    @staticmethod
    def instr_2_mean_nov_2014(df) -> None:
        """Calculate the mean for INSTRUMENT2 for November 2014."""
        print("Calculate the mean for INSTRUMENT2 for November 2014.")
        df_grouped_year_month = df.groupBy(
            "INSTRUMENT_NAME", year("DATE").alias("YEAR"), month("DATE").alias("MONTH")
        ).agg({"VALUE": "mean"})

        target_year = 2014
        target_month = 11
        target_instrument = "INSTRUMENT2"

        # Filter the DataFrame
        mean_value_INST2_2014_11 = df_grouped_year_month.filter(
            (col("YEAR") == target_year)
            & (col("MONTH") == target_month)
            & (col("INSTRUMENT_NAME") == target_instrument)
        )

        mean_value_INST2_2014_11.show()

    @staticmethod
    def instr_3_statistics(df) -> None:
        """Perform statistical on-the-fly calculations for INSTRUMENT3."""

        # Filter only the rows where 'INSTRUMENT_NAME' is 'INSTRUMENT3'
        instrument3_df = df.filter(col("INSTRUMENT_NAME") == "INSTRUMENT3")

        # Apply describe()
        instrument3_stats = instrument3_df.describe()

        # keep only meaningful columns
        instrument3_stats.select("summary", "VALUE").show()

    @staticmethod
    def sum_newest_10_elems(df, **kwargs):
        """Calculate the sum of the newest 10 elements in terms of the date."""
        pass

    @staticmethod
    def run(df) -> None:
        CalculationEngine.instr_1_mean(df)
        CalculationEngine.instr_2_mean_nov_2014(df)
        CalculationEngine.instr_3_statistics(df)
        CalculationEngine.sum_newest_10_elems(df)
