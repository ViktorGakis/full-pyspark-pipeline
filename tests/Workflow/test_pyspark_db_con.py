from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Table name
TABLE_NAME = "test_table"

# Define the schema
SCHEMA_DB = StructType(
    [
        StructField("ID", IntegerType(), False),
        StructField("NAME", StringType(), True),
        StructField("MULTIPLIER", DoubleType(), True),
    ]
)

# Sample data
data = [
    (1, "INSTRUMENT5", 5.19),
    (2, "INSTRUMENT4", 5.05),  # ... (add the rest of your data)
]


def test_database_operations(spark_session, config):
    # Create DataFrame
    df = spark_session.createDataFrame(data, schema=SCHEMA_DB).orderBy("ID")

    # Write data to the database
    df.write.jdbc(
        url=config.MYSQL_PROPERTIES["url"],
        table=config.TABLE_NAME,
        mode="overwrite",  # Use "append" if needed
        properties=config.MYSQL_PROPERTIES,
    )

    # Read data from the database
    df_read = (
        spark_session.read.format("jdbc")
        .option("url", config.MYSQL_PROPERTIES["url"])
        .option("dbtable", TABLE_NAME)
        .option("user", config.MYSQL_PROPERTIES["user"])
        .option("password", config.MYSQL_PROPERTIES["password"])
        .load()
    )

    # Show some data (for debugging)
    df_read.show()

    # Test: Validate if data read matches expected values
    assert df_read.count() == len(data)
    assert df_read.where("ID = 1").select("NAME").collect()[0][0] == "INSTRUMENT5"

    # Teardown: Drop the table and stop the Spark session
    spark_session.sql(f"DROP TABLE IF EXISTS {TABLE_NAME}")
