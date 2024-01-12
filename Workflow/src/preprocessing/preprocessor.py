from datetime import _Date, datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, dayofweek, to_date


class PreprocessData:
    @staticmethod
    def date_transform(
        df: DataFrame, col_date_str="DATE", date_format_str="dd-MMM-yyyy"
    ) -> DataFrame:
        return df.withColumn(col_date_str, to_date(df[col_date_str], date_format_str))

    @staticmethod
    def date_sorting(df: DataFrame) -> DataFrame:
        return df.orderBy(col("DATE").desc())

    @staticmethod
    def business_date_validation(df: DataFrame) -> DataFrame:
        df_with_dayofweek: DataFrame = df.withColumn("DAY_OF_WEEK", dayofweek("DATE"))
        return df_with_dayofweek.filter(
            (col("DAY_OF_WEEK") >= 2) & (col("DAY_OF_WEEK") <= 6)
        )

    @staticmethod
    def cutoff_after_current_date(df: DataFrame, config) -> DataFrame:
        current_date: _Date = datetime.strptime(config.CURRENT_DATE, "%d-%m-%Y").date()
        cutoff_date = current_date
        return df.filter(col("DATE") <= cutoff_date)

    @staticmethod
    def run(df: DataFrame, config) -> DataFrame:
        df = PreprocessData.date_transform(df)
        df = PreprocessData.date_sorting(df)
        df = PreprocessData.business_date_validation(df)
        df = PreprocessData.cutoff_after_current_date(df, config)
        return df
