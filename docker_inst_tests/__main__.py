import os
import sys

# Get the parent directory of the current file (tests)
current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))

from src import (
    drop_table,
    table_preparation,
    test_mysql_conx,
    test_pyspark_con,
    test_pyspark_db_conx,
)


def main():
    test_mysql_conx()
    table_preparation()
    # test_pyspark_con()
    test_pyspark_db_conx()
    drop_table()


if __name__ == "__main__":
    main()
