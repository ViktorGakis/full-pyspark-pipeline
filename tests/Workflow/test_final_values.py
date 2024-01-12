import pytest
from pyspark.sql import DataFrame, Row

from Workflow.src import FinalValues


def mock_db_query_function(instrument_name, spark):
    # Mocked multipliers for different instruments
    multipliers = {
        "INSTRUMENT1": 1.5,
        "INSTRUMENT2": 2.0,
        "INSTRUMENT3": None,  # Assume no entry in the database
    }

    multiplier = multipliers.get(instrument_name, None)
    if multiplier is None:
        return spark.createDataFrame([], schema="MULTIPLIER DOUBLE")
    else:
        return spark.createDataFrame([Row(MULTIPLIER=multiplier)])


@pytest.fixture(scope="module")
def mock_db_query(spark):
    def _query_func(instrument_name):
        return mock_db_query_function(instrument_name, spark)

    return _query_func


def test_final_value_calculation(spark):
    # Sample data
    rows = [
        {"INSTRUMENT_NAME": "INSTRUMENT1", "VALUE": 100},
        {"INSTRUMENT_NAME": "INSTRUMENT2", "VALUE": 200},
        {"INSTRUMENT_NAME": "INSTRUMENT3", "VALUE": 300},
    ]
    df = spark.createDataFrame(rows)

    # Multipliers DataFrame
    multipliers_data = [
        ("INSTRUMENT1", 1.5),
        ("INSTRUMENT2", 2.0),
        ("INSTRUMENT3", None),
    ]
    multipliers_df = spark.createDataFrame(
        multipliers_data, ["INSTRUMENT_NAME", "MULTIPLIER"]
    )

    # FinalValues instance with multipliers DataFrame
    final_values_calculator = FinalValues(multipliers_df)
    final_df = final_values_calculator.final_values_cal(df)
    results = [
        (row["INSTRUMENT_NAME"], row["FINAL_VALUE"]) for row in final_df.collect()
    ]
    assert results == [("INSTRUMENT1", 150), ("INSTRUMENT2", 400), ("INSTRUMENT3", 300)]


