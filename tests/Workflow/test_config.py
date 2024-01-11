import os
from pathlib import Path

from Workflow.src.config import Config


def test_get_config_on_the_fly():
    # Set a test environment variable
    os.environ["TEST_VARIABLE"] = "test_value"

    # Test if Config class retrieves the correct value
    assert Config.get_env("TEST_VARIABLE") == "test_value"

    # Cleanup - remove the test environment variable
    del os.environ["TEST_VARIABLE"]


def test_get_config_from_env_file():
    from dotenv import load_dotenv

    # load .env file
    load_dotenv()

    # Test if Config class retrieves the correct value
    assert Config.get_env("MYSQL_ROOT_USER") == "root"


def test_proper_version_mysql_connector():
    assert Config.get_env("MYSQL_CONNECTOR_FILENAME") == "mysql-connector-j-8.2.0.jar"


def test_mysql_connector_path_existence():
    assert Path(Config.get_env("MYSQL_CONNECTOR_PATH")).exists() is True
