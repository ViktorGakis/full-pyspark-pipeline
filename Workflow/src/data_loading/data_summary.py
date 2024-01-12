from pyspark.sql import DataFrame


class DataSummary:
    @staticmethod
    def display_summary(df: DataFrame) -> None:
        df.printSchema()
        df.show(5)
