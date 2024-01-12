from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


class LoadTxtData:
    def __init__(
        self, spark: SparkSession, schema: StructType, filepath_str: str
    ) -> None:
        self.spark: SparkSession = spark
        self.schema: StructType = schema
        self.filepath_str: str = filepath_str

    def load_source_file(self) -> DataFrame:
        return self.spark.read.option("delimiter", ",").csv(
            self.filepath_str, header=False, schema=self.schema
        )
