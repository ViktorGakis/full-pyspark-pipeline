# Workflow/src/pipeline.py
from .src.config import Config
from .src.database.manager import MysqlManager
from .src.spark.session import Spark


def main():
    config = Config()
    spark = Spark(config).create()


if __name__ == "__main__":
    main()
