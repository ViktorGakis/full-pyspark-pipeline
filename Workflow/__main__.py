from pyspark.sql import DataFrame, SparkSession
from src import (
    CalculationEngine,
    Config,
    DatabaseService,
    DataPreprocessor,
    DataSummary,
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

    db_service = DatabaseService(spark, config)

    multipliers_df = db_service.get_multipliers_df()

    final_values_calculator = FinalValues(multipliers_df)

    final_df = final_values_calculator.final_values_cal(df_processed)

    # Display final DataFrame
    if verbose:
        final_df.show()


if __name__ == "__main__":
    main()
