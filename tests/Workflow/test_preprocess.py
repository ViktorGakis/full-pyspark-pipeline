from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, max, min
from pyspark.sql.types import DateType

from Workflow.src import DataPreprocessor


def test_date_transform(df_txt) -> None:
    df_txt_trans: DataFrame = DataPreprocessor.date_transform(df_txt)
    assert df_txt_trans.schema["DATE"].dataType == DateType()



