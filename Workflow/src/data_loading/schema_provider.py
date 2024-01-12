from pyspark.sql.types import DoubleType, StringType, StructField, StructType


class TxtSchemaProvider:
    schema = StructType(
        [
            StructField(name="INSTRUMENT_NAME", dataType=StringType(), nullable=True),
            StructField(name="DATE", dataType=StringType(), nullable=True),
            StructField(name="VALUE", dataType=DoubleType(), nullable=True),
        ]
    )
