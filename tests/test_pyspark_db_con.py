from os import getenv
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

MYSQL_CONNECTOR_FILENAME: str = "mysql-connector-j-8.2.0.jar"
MYSQL_CONNECTOR_PATH: str = f"/app/mysql_connector/{MYSQL_CONNECTOR_FILENAME}"


DB_CON_DICT = dict(
    user=getenv("MYSQL_ROOT_USER"),
    password=getenv("MYSQL_ROOT_PASSWORD"),
    host=getenv("HOST"),
    port=getenv("MYSQL_DOCKER_PORT"),  # type: ignore
    database=getenv("MYSQL_DATABASE"),
)

MYSQL_PROPERTIES = {
    "driver": "com.mysql.cj.jdbc.Driver",
    "url": "jdbc:mysql://{host}:{port}/{database}".format(**DB_CON_DICT),  # type: ignore
    "user": DB_CON_DICT["user"],  # type: ignore
    "password": DB_CON_DICT["password"],  # type: ignore
}


# Table name
TABLE_NAME = "test_table"


def test_pyspark_db_conx():
    print(f"{MYSQL_PROPERTIES=}")
    print(f"MYSQL driver path existence: {Path(MYSQL_CONNECTOR_PATH).exists()}")
    import findspark

    findspark.init()

    findspark.add_jars(MYSQL_CONNECTOR_PATH)

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("DatabaseConnection")
        # .config("spark.jars", MYSQL_CONNECTOR_PATH)
        # .config("spark.driver.extraClassPath", MYSQL_CONNECTOR_PATH)
        # .config("spark.executor.extraClassPath", MYSQL_CONNECTOR_PATH)
        .getOrCreate()
    )
    try:
        # Read data from the database
        df = (
            spark.read.format("jdbc")
            .options(**MYSQL_PROPERTIES, dbtable=TABLE_NAME)
            .load()
        )

        # Show the data
        df.show()

    except Exception as e:
        print(f"Error connecting to the database: {str(e)}")
    finally:
        # Stop the Spark session
        spark.stop()


if __name__ == "__main__":
    test_pyspark_db_conx()
