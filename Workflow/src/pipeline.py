from pyspark.sql import DataFrame, SparkSession

from .calculation_engine import CalculationEngine
from .data_loading import DataSummary, LoadTxtData, TxtSchemaProvider
from .data_preprocessing import DataPreprocessor
from .database import DatabaseInjector, DatabaseService, DBSchemaProvider, MysqlManager
from .final_values import FinalValues
from .spark import Spark


class Pipeline:
    def __init__(self, config, verbose=False):
        self.config = config
        self.verbose: bool = verbose
        self.spark: SparkSession = Spark(config).create()

    def log(self, message) -> None:
        if self.verbose:
            print(message)

    def load_data(self) -> DataFrame:
        self.log(
            "------------------------------\nLOADING .TXT FILE\n------------------------------"
        )
        df_txt: DataFrame = LoadTxtData(
            self.spark, TxtSchemaProvider.schema, self.config.TXT_FILE_REL_PATH_STR
        ).load_source_file()
        DataSummary.display_summary(df_txt) if self.verbose else None
        return df_txt

    def preprocess_data(self, df_txt) -> DataFrame:
        self.log(
            "\n------------------------------\nDF_PREPROCESSED\n------------------------------"
        )
        df_processed: DataFrame = DataPreprocessor.run(df_txt, self.config)
        DataSummary.display_summary(df_processed) if self.verbose else None
        return df_processed

    def run_calculations(self, df_processed) -> None:
        self.log(
            "\n------------------------------\nCalculationEngine\n------------------------------"
        )
        CalculationEngine.run(df_processed)

    def setup_database(self) -> None:
        MysqlManager(self.config).setup()

    def inject_data(self, data, schema) -> None:
        db_injector = DatabaseInjector(self.spark, self.config)
        db_injector.inject_data(data, schema, "INSTRUMENT_PRICE_MODIFIER")

    def fetch_multipliers(self):
        db_service = DatabaseService(self.spark, self.config)
        return db_service.get_multipliers_df()

    def calculate_final_values(self, df_processed, multipliers_df):
        final_values_calculator = FinalValues(multipliers_df)
        return final_values_calculator.final_values_cal(df_processed)

    def run_pipeline(self, data=None):
        df_txt: DataFrame = self.load_data()
        df_processed: DataFrame = self.preprocess_data(df_txt)
        self.run_calculations(df_processed)

        self.setup_database()
        self.inject_data(data, DBSchemaProvider.schema) if data else None

        multipliers_df = self.fetch_multipliers()
        final_df = self.calculate_final_values(df_processed, multipliers_df)

        self.log("Final DataFrame:")
        return final_df
