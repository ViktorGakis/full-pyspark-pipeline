from abc import ABC, abstractmethod


class DatabaseManager(ABC):
    @abstractmethod
    def create_db(self, *args, **kwargs):
        pass


class MysqlManager(DatabaseManager):
    def __init__(self, spark_session):
        self.spark_session = spark_session

    def create_db(self, db_name) -> None:
        # Use the provided Spark session to create the database
        self.spark_session.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")