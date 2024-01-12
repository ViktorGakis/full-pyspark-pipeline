from abc import ABC, abstractmethod


class DatabaseManager(ABC):
    @abstractmethod
    def create_db(self, *args, **kwargs):
        pass

    @abstractmethod
    def create_table(self, *args, **kwargs):
        pass
