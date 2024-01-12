from os import getenv
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv

load_dotenv()


class Config:
    CWD: Path = Path("/app/")
    MYSQL_ROOT_USER: str | None = getenv("MYSQL_ROOT_USER")
    MYSQL_ROOT_HOST: str | None = getenv("MYSQL_ROOT_HOST")
    MYSQL_ROOT_PASSWORD: str | None = getenv("MYSQL_ROOT_PASSWORD")
    MYSQL_USER: str | None = getenv("MYSQL_USER")
    MYSQL_PASSWORD: str | None = getenv("MYSQL_PASSWORD")
    HOST: str | None = getenv("HOST")
    MYSQL_DATABASE: str | None = getenv("MYSQL_DATABASE")
    MYSQL_HOST_PORT: str | None = getenv("MYSQL_HOST_PORT")
    MYSQL_DOCKER_PORT: str | None = getenv("MYSQL_DOCKER_PORT")
    PYTHON_HOST_PORT: str | None = getenv("PYTHON_HOST_PORT")
    PYTHON_DOCKER_PORT: str | None = getenv("PYTHON_DOCKER_PORT")
    PHPMYADMIN_HOST_PORT: str | None = getenv("PHPMYADMIN_HOST_PORT")
    PHPMYADMIN_DOCKER_PORT: str | None = getenv("PHPMYADMIN_DOCKER_PORT")
    MYSQL_CONNECTOR_FILENAME: str | None = getenv("MYSQL_CONNECTOR_FILENAME")
    MYSQL_CONNECTOR_PATH: str | None = getenv("MYSQL_CONNECTOR_PATH")
    TABLE_NAME: str | None = getenv("TABLE_NAME")
    appName: str | None = getenv("appName")
    TXT_FILE_REL_PATH_STR: str | None = getenv("TXT_FILE_REL_PATH_STR")
    CURRENT_DATE: str | None = getenv("CURRENT_DATE")
    MAX_BUSINESS_WEEK_DAY: str | None = getenv("MAX_BUSINESS_WEEK_DAY")
    MIN_BUSINESS_WEEK_DAY: str | None = getenv("MIN_BUSINESS_WEEK_DAY")

    # Database configuration
    @property
    def DB_CON_DICT(self) -> dict[str, Any]:
        return {
            "user": self.MYSQL_ROOT_USER,
            "password": self.MYSQL_ROOT_PASSWORD,
            "host": self.HOST,
            "port": self.MYSQL_DOCKER_PORT,
            "database": self.MYSQL_DATABASE,
        }

    @property
    def MYSQL_PROPERTIES(self) -> dict[str, Any]:
        return {
            "driver": "com.mysql.cj.jdbc.Driver",
            "url": f"jdbc:mysql://{self.DB_CON_DICT['host']}:{self.DB_CON_DICT['port']}/{self.DB_CON_DICT['database']}",
            "user": self.DB_CON_DICT["user"],
            "password": self.DB_CON_DICT["password"],
        }

    @staticmethod
    def get_env(key: str, default=None) -> Optional[str]:
        return getenv(key, default)

        # Sample data and schema


