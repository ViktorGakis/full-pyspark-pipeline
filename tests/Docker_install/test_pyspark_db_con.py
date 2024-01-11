import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from Workflow.src.config import Config

USER: str = Config.get_env("MYSQL_ROOT_USER")
PASSWORD: str = Config.get_env("MYSQL_ROOT_PASSWORD")
HOST: str = Config.get_env("HOST")
PORT: str = Config.get_env("MYSQL_DOCKER_PORT")
DATABASE: str = Config.get_env("MYSQL_DATABASE")

# MySQL connector configuration
MYSQL_CONNECTOR_FILENAME: str = Config.get_env("MYSQL_CONNECTOR_FILENAME")
MYSQL_CONNECTOR_PATH: str = Config.get_env("MYSQL_CONNECTOR_PATH")

# Database configuration
DB_CON_DICT = dict(
    user=USER,
    password=PASSWORD,
    host=HOST,
    port=PORT,
    database=DATABASE,
)

MYSQL_PROPERTIES = {
    "driver": "com.mysql.cj.jdbc.Driver",
    "url": f"jdbc:mysql://{DB_CON_DICT['host']}:{DB_CON_DICT['port']}/{DB_CON_DICT['database']}",
    "user": DB_CON_DICT["user"],
    "password": DB_CON_DICT["password"],
}

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


@pytest.fixture(scope="module")
def spark_session():
    # Initialize Spark
    import findspark

    findspark.init()
    findspark.add_jars(MYSQL_CONNECTOR_PATH)

    spark = (
        SparkSession.builder.appName("DatabaseConnection")
        .config("spark.jars", MYSQL_CONNECTOR_PATH)
        .config("spark.driver.extraClassPath", MYSQL_CONNECTOR_PATH)
        .config("spark.executor.extraClassPath", MYSQL_CONNECTOR_PATH)
        .getOrCreate()
    )

    yield spark

    # Teardown: Drop the table and stop the Spark session
    spark.sql(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    spark.stop()


def test_database_operations(spark_session):
    # Create DataFrame
    df = spark_session.createDataFrame(data, schema=SCHEMA_DB).orderBy("ID")

    # Write data to the database
    df.write.jdbc(
        url=MYSQL_PROPERTIES["url"],
        table=TABLE_NAME,
        mode="overwrite",  # Use "append" if needed
        properties=MYSQL_PROPERTIES,
    )

    # Read data from the database
    df_read = (
        spark_session.read.format("jdbc")
        .option("url", MYSQL_PROPERTIES["url"])
        .option("dbtable", TABLE_NAME)
        .option("user", MYSQL_PROPERTIES["user"])
        .option("password", MYSQL_PROPERTIES["password"])
        .load()
    )

    # Show some data (for debugging)
    df_read.show()

    # Test: Validate if data read matches expected values
    assert df_read.count() == len(data)
    assert df_read.where("ID = 1").select("NAME").collect()[0][0] == "INSTRUMENT5"
