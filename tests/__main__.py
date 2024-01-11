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
