from .manager import DatabaseManager


class ConcreteMysqlManager(DatabaseManager):
    def create_db(self, *args, **kwargs) -> None:
        # Concrete implementation for MySQL
        pass
