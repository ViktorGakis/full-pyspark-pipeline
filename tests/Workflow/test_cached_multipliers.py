import time

from pyspark.sql import DataFrame
from pytest import fixture

from Workflow.src.pipeline import Pipeline

DATA: list[tuple[int, str, float]] = [
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


@fixture
def pipeline(spark, config):
    pipeline = Pipeline(spark, config, verbose=True)

    yield pipeline

    pipeline.spark.stop()


def setup_test_table(config, mysqlmanager) -> None:
    # Logic to create and populate the INSTRUMENT_PRICE_MODIFIER_TEST table
    mysqlmanager.create_table(table=f"{config.TABLE_NAME}_TEST")


def inject_data(
    pipeline,
    config,
    data=DATA,
) -> None:
    pipeline.inject_data(data, table=f"{config.TABLE_NAME}_TEST")


def test_fetch_multipliers_caching(pipeline, mysqlmanager, config) -> None:
    setup_test_table(config, mysqlmanager)
    inject_data(pipeline, config, DATA)

    # First call to populate cache
    multipliers_df_1: DataFrame = pipeline.fetch_multipliers()
    assert multipliers_df_1 is not None

    # Short delay within caching window
    time.sleep(2)

    # Second call should fetch from cache
    multipliers_df_2: DataFrame = pipeline.fetch_multipliers()
    assert multipliers_df_2 is not None
    # Checking if dataframes are the same
    assert multipliers_df_1._jdf.equals(multipliers_df_2._jdf)

    # Delay longer than caching window
    time.sleep(6)

    # Third call should fetch new data
    multipliers_df_3: DataFrame = pipeline.fetch_multipliers()
    assert multipliers_df_3 is not None
    # Dataframes should be different
    assert not multipliers_df_2._jdf.equals(multipliers_df_3._jdf)


def test_table_drop(mysqlmanager, config) -> None:
    # Teardown: Drop the table and stop the Spark session
    mysqlmanager.drop_table(table=f"{config.TABLE_NAME}_TEST")
    # Verification: Check if the table has been dropped
    with mysqlmanager.connection.cursor() as cursor:
        cursor.execute(f"SHOW TABLES LIKE '{config.TABLE_NAME}_TEST'")
        result = cursor.fetchone()
        # Table should not exist
        assert result is None
