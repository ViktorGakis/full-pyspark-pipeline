from pyspark.sql.functions import col, day, mean, month, year


class CalculationEngine:
    """Class for performing various calculations on financial instruments."""

    @staticmethod
    def instr_1_mean(df):
        """Calculate the mean for INSTRUMENT1."""
        print("mean for INSTRUMENT1.")
        (
            df.groupBy("INSTRUMENT_NAME")
            .agg(mean("VALUE").alias("MEAN"))
            .filter(col("INSTRUMENT_NAME") == "INSTRUMENT1")
        ).show()

    @staticmethod
    def instr_2_mean_nov_2014(df, **kwargs):
        """Calculate the mean for INSTRUMENT2 for November 2014."""


    @staticmethod
    def instr_3_statistics(df, **kwargs):
        """Perform statistical on-the-fly calculations for INSTRUMENT3."""
        pass

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
