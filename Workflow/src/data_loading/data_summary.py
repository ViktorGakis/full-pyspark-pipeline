from pyspark.sql import DataFrame


class DataSummary:
    @staticmethod
    def display_summary(df: DataFrame, rows=10) -> None:
        df.printSchema()
        df.show(rows)
