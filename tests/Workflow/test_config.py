import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def test_get_config_on_the_fly(config):
    # Set a test environment variable
    os.environ["TEST_VARIABLE"] = "test_value"

    # Test if Config class retrieves the correct value
    assert config.get_env("TEST_VARIABLE") == "test_value"

    # Cleanup - remove the test environment variable
    del os.environ["TEST_VARIABLE"]


def test_get_config_from_env_file_1(config):
    # Test if Config class retrieves the correct value
    assert config.get_env("MYSQL_ROOT_USER") == "root"


def test_get_config_from_env_file_2(config):
    # Test if Config class retrieves the correct value
    assert config.MYSQL_ROOT_USER == "root"


def test_proper_version_mysql_connector(config):
    assert config.MYSQL_CONNECTOR_FILENAME == "mysql-connector-j-8.2.0.jar"


def test_mysql_connector_path_existence(config):
    assert Path(config.MYSQL_CONNECTOR_PATH).exists() is True
