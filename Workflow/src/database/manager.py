from abc import ABC, abstractmethod


class DatabaseManager(ABC):
    @abstractmethod
    def create_db(self, *args, **kwargs):
        pass


class MysqlManager(DatabaseManager):
    def create_db(self, db_name, spark) -> None:
        # Use the provided Spark session to create the database
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
