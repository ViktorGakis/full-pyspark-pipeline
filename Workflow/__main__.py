import ast
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from src import Config, Pipeline

config = Config()


def main(verbose: bool = False, test_multiplier_data: bool = True) -> None:
    config = Config()
    pipeline = Pipeline(config, verbose)

    # Define data to inject (if any) in mysql table INSTRUMENT_PRICE_MODIFIER
    # if no data is given, then whatever data exists in INSTRUMENT_PRICE_MODIFIER
    # will be used

    if test_multiplier_data:
        MULTIPLIER_TEST_DATA_PATH = Path(
            config.get_env("MULTIPLIER_TEST_DATA_PATH_STR")
        )
        if MULTIPLIER_TEST_DATA_PATH.exists():
            with MULTIPLIER_TEST_DATA_PATH.open("r") as f:
                data = ast.literal_eval(f.read())
                print(
                    "\n\n--------- The test data is engineered to only contain INSTRUMENT2. ---------\n\n"
                )
        else:
            print(f"The test data file {MULTIPLIER_TEST_DATA_PATH} was not found")

    # test data is engineered to not have INSTRUMENT1 and INSTRUMENT3
    df: DataFrame = pipeline.run_pipeline(data).drop(col("ID"))

    # Filter and limit rows for each instrument
    instrument1_df: DataFrame = df.filter(df["INSTRUMENT_NAME"] == "INSTRUMENT1").limit(
        5
    )
    instrument2_df: DataFrame = df.filter(df["INSTRUMENT_NAME"] == "INSTRUMENT2").limit(
        5
    )
    instrument3_df: DataFrame = df.filter(df["INSTRUMENT_NAME"] == "INSTRUMENT3").limit(
        5
    )

    # Union the DataFrames
    instrument1_df.union(instrument2_df).union(instrument3_df).show()


if __name__ == "__main__":
    main(True, test_multiplier_data=True)
