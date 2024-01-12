from pyspark.sql import DataFrame, SparkSession
from src import (
    CalculationEngine,
    Config,
    DatabaseInjector,
    DatabaseService,
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

    # Initialize DatabaseInjector
    db_injector = DatabaseInjector(spark, config)

    # Sample data and schema
    data: list[tuple[int, str, float]] = [
        (1, "INSTRUMENT5", 5.19),
        (2, "INSTRUMENT4", 5.05),
        (3, "INSTRUMENT2", 4.4),
        (4, "INSTRUMENT4", 2.25),
        (5, "INSTRUMENT5", 1.75),
        (6, "INSTRUMENT6", 9.91),
        (7, "INSTRUMENT4", 4.5),
        (8, "INSTRUMENT2", 9.24),
        (9, "INSTRUMENT6", 5.83),
        (10, "INSTRUMENT5", 1.34),
        (11, "INSTRUMENT6", 8.89),
        (12, "INSTRUMENT2", 2.59),
        (13, "INSTRUMENT5", 4.04),
        (14, "INSTRUMENT4", 8.58),
        (15, "INSTRUMENT2", 8.64),
        (16, "INSTRUMENT2", 3.99),
        (17, "INSTRUMENT2", 6.82),
        (18, "INSTRUMENT2", 7.7),
        (19, "INSTRUMENT4", 4.44),
        (20, "INSTRUMENT4", 8.01),
    ]

    # Inject data into INSTRUMENT_PRICE_MODIFIER table
    db_injector.inject_data(data, DBSchemaProvider.schema, "INSTRUMENT_PRICE_MODIFIER")

    db_service = DatabaseService(spark, config)

    multipliers_df = db_service.get_multipliers_df()

    if verbose:
        print("Multipliers table")
        multipliers_df.show()

    final_values_calculator = FinalValues(multipliers_df)

    final_df = final_values_calculator.final_values_cal(df_processed)

    # Display final DataFrame
    if verbose:
        final_df.show()


if __name__ == "__main__":
    main(True)
