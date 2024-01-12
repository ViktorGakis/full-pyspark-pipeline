from pyspark.sql import DataFrame

from Workflow.src.data_loading import LoadTxtData, TxtSchemaProvider


def test_loadtxtdata(spark, config) -> None:
    df_txt: DataFrame = LoadTxtData(
        spark, TxtSchemaProvider.schema, config.TXT_FILE_REL_PATH_STR  # type: ignore
    ).load_source_file()
    assert df_txt.isEmpty() == False
