from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


class DBSchemaProvider:
    schema = StructType(
        [
            StructField(
                "ID", IntegerType(), False
            ),  # False indicates that the field is not nullable
            StructField(
                "NAME", StringType(), True
            ),  # True indicates that the field is nullable
            StructField("MULTIPLIER", DoubleType(), True),
        ]
    )
