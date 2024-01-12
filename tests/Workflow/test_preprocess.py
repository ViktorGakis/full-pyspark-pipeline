from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, max, min
from pyspark.sql.types import DateType

from Workflow.src import DataPreprocessor


def test_date_transform(df_txt) -> None:
    df_txt_trans: DataFrame = DataPreprocessor.date_transform(df_txt)
    assert df_txt_trans.schema["DATE"].dataType == DateType()


def test_business_date_validation(df_txt, config) -> None:
    df_txt_trans: DataFrame = DataPreprocessor.date_transform(df_txt)
    df_txt_trans = DataPreprocessor.date_sorting(df_txt_trans)
    df_txt_trans = DataPreprocessor.business_date_validation(df_txt_trans)
    max_value = df_txt_trans.agg(max(col("DAY_OF_WEEK")).alias("MAX")).collect()[0][
        "MAX"
    ]
    min_value = df_txt_trans.agg(min(col("DAY_OF_WEEK")).alias("MIN")).collect()[0][
        "MIN"
    ]

    assert (min_value, max_value) == (
        int(config.MIN_BUSINESS_WEEK_DAY),
        int(config.MAX_BUSINESS_WEEK_DAY),
    )

