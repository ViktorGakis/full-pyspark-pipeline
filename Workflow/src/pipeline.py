from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from .calculation_engine import CalculationEngine
from .config import Config
from .data_loading import DataSummary, LoadTxtData, TxtSchemaProvider
from .data_preprocessing import DataPreprocessor
from .database import (
    DatabaseInjector,
    DatabaseService,
    DBSchemaProvider,
    MysqlManager,
    cache_query,
)
from .final_values import FinalValues
from .spark import Spark


class Pipeline:
    def __init__(
        self,
        spark: SparkSession,
        config: Config,
        verbose: bool = False,
    ) -> None:
        self.config: Config = config
        self.verbose: bool = verbose
        self.spark: SparkSession = (
            Spark(config=config).create() if spark is None else spark
        )

    def log(self, message) -> None:
        if self.verbose:
            print(message)

    def load_data(self) -> DataFrame:
        self.log(
            "------------------------------\nLOADING .TXT FILE\n------------------------------"
        )
        df_txt: DataFrame = LoadTxtData(
            spark=self.spark,
            schema=TxtSchemaProvider.schema,
            filepath_str=self.config.TXT_FILE_REL_PATH_STR,
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

    def inject_data(
        self, data, table: Optional[str] = None, schema: Optional[StructType] = None  # type: ignore
    ) -> None:
        if not table:
            table: str = self.config.TABLE_NAME
        if not schema:
            schema: StructType = DBSchemaProvider.schema
        db_injector = DatabaseInjector(spark=self.spark, config=self.config)
        db_injector.inject_data(data=data, schema=schema, table_name=table)

    @cache_query(seconds=5)  # 5 also by default
    def fetch_multipliers(self):
        db_service = DatabaseService(spark_session=self.spark, config=self.config)
        return db_service.get_multipliers_df()

    def calculate_final_values(self, df_processed, multipliers_df):
        final_values_calculator = FinalValues(multipliers_df)
        return final_values_calculator.final_values_cal(df_processed)

    def run_pipeline(self, data=None):
        df_txt: DataFrame = self.load_data()
        df_processed: DataFrame = self.preprocess_data(df_txt=df_txt)
        self.run_calculations(df_processed=df_processed)

        self.setup_database()
        self.inject_data(
            data=data,
        ) if data else None

        multipliers_df = self.fetch_multipliers()
        final_df = self.calculate_final_values(
            df_processed=df_processed, multipliers_df=multipliers_df
        )

        self.log("Final DataFrame:")
        return final_df
