from .test_db_con import test_mysql_conx
from .test_prepare_table import drop_table, table_preparation
from .test_pyspark_db_con import test_pyspark_db_conx
from .test_pyspark_inst import test_pyspark_con

def run_all():
    test_mysql_conx()
    table_preparation()
    test_pyspark_con()
    test_pyspark_db_conx()
    drop_table()


if __name__ == "__main__":
    run_all()