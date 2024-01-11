from os import getenv

from dotenv import load_dotenv

load_dotenv()


class Config:
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

    @staticmethod
    def get_env(key: str, default=None):
        return getenv(key, default)
