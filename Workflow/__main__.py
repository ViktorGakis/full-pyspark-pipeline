# Workflow/src/pipeline.py
from .src.config import Config
from .src.database.manager import MysqlManager
from .src.spark.session import Spark

def main():
    config = Config()
    spark = Spark(config).create()
    spark_session = spark.create()

def main() -> None:
    # Create a Spark session
    

    
    
    
    
    # Initialize the database manager with the Spark session
    db_manager = MysqlManager(spark)

    # Create the database
    db_manager.create_db(Config.MYSQL_DATABASE)

    # Other pipeline operations...


if __name__ == "__main__":
    main()
