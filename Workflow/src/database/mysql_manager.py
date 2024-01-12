import pymysql

from .manager import DatabaseManager


class MysqlManager(DatabaseManager):
    def __init__(self, config) -> None:
        super().__init__()
        self.config = config
        self.connection = None

    def create_conx(self) -> None:
        try:
            self.connection = pymysql.connect(
                host=self.config.HOST,
                port=int(self.config.MYSQL_DOCKER_PORT),
                user=self.config.MYSQL_ROOT_USER,
                password=self.config.MYSQL_ROOT_PASSWORD,
                database=self.config.MYSQL_DATABASE,
            )
        except pymysql.Error as e:
            # Handle the connection error, e.g., log or raise an exception
            raise RuntimeError(f"Failed to connect to the database: {e}")

    def close_conx(self) -> None:
        if self.connection is not None:
            try:
                self.connection.close()
            except pymysql.Error as e:
                # Handle the connection closure error, e.g., log or raise an exception
                raise RuntimeError(f"Failed to close the database connection: {e}")

    def create_db(self) -> None:
        """Create a database if it does not exist."""
        try:
            # Create the database if it does not exist
            with self.connection.cursor() as cursor:
                cursor.execute(
                    f"CREATE DATABASE IF NOT EXISTS {self.config.MYSQL_DATABASE}"
                )
        except pymysql.Error as e:
            # Handle the database creation error, e.g., log or raise an exception
            raise RuntimeError(f"Failed to create the database: {e}")

    def create_table(self) -> None:
        """Create a table if it does not exist."""
        try:
            # Use the database
            with self.connection.cursor() as cursor:
                cursor.execute(f"USE {self.config.MYSQL_DATABASE}")

                # Create the table if it does not exist
                cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self.config.TABLE_NAME} (
                        ID INT PRIMARY KEY,
                        NAME VARCHAR(255),
                        MULTIPLIER DOUBLE
                    )
                    """
                )
        except pymysql.Error as e:
            # Handle the table creation error, e.g., log or raise an exception
            raise RuntimeError(f"Failed to create the table: {e}")

    def setup(self) -> None:
        """Set up the database and table."""
        try:
            self.create_conx()
            self.create_db()
            self.create_table()
        except RuntimeError as e:
            # Handle the runtime error, e.g., log or raise an exception
            print(f"Error during setup: {e}")
        finally:
            self.close_conx()
