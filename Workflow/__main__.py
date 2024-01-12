# Workflow/src/pipeline.py
from .src.config import Config
from .src.spark.session import Spark


def main():
    config = Config()
    spark = Spark(config).create()
    EXAMPLE_INPUT_PATH

if __name__ == "__main__":
    main()
