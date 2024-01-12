from pyspark.sql import DataFrame, SparkSession
from src import (
    CalculationEngine,
    Config,
    DatabaseQueryService,
    DataPreprocessor,
    DataSummary,
    DBSchemaProvider,
    FinalValues,
    LoadTxtData,
    MysqlManager,
    Spark,
    TxtSchemaProvider,
)

config = Config()


def main(verbose: bool = False) -> None:
    config = Config()
    spark: SparkSession = Spark(config).create()
    if verbose:
        print("------------------------------")
        print("LOADING .TXT FILE")
        print("------------------------------")
    df_txt: DataFrame = LoadTxtData(
        spark, TxtSchemaProvider.schema, config.TXT_FILE_REL_PATH_STR  # type: ignore
    ).load_source_file()

    if verbose:
        DataSummary.display_summary(df_txt)
        print("\n")
        print("------------------------------")
        print("DF_PREPROCESSED")
        print("------------------------------")
    df_processed: DataFrame = DataPreprocessor.run(df_txt, config)

    if verbose:
        DataSummary.display_summary(df_processed)
        print("\n")
        print("------------------------------")
        print("CalculationEngine")
        print("------------------------------")
    CalculationEngine.run(df_processed)

    MysqlManager(config).setup()

    query_db = DatabaseQueryService(
        spark_session=spark,
        table_name=config.TABLE_NAME,
        properties=config.MYSQL_PROPERTIES,
    ).handle_query

    # Initialize FinalValues with rows and query_db function

    final_values_calculator = FinalValues(query_db)
    final_df = final_values_calculator.final_values_cal(spark)
    final_df.show()


if __name__ == "__main__":
    main()
