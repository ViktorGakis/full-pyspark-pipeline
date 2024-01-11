from abc import ABC, abstractmethod


class DatabaseManager(ABC):
    @abstractmethod
    def create_db(self, *args, **kwargs):
        pass


class MysqlManager(DatabaseManager):
    def create_db(self, *args, **kwargs):
        # Implementation for creating MySQL database
        pass
