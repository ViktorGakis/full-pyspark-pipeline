from unittest.mock import Mock, patch

import pytest

from Workflow.src.database import MysqlManager


@pytest.fixture
def config_mock() -> Mock:
    return Mock(
        HOST="localhost",
        MYSQL_DOCKER_PORT=3306,
        MYSQL_ROOT_USER="root",
        MYSQL_ROOT_PASSWORD="password",
        MYSQL_DATABASE="testdb",
        TABLE_NAME="testtable",
    )


@pytest.fixture
def mysql_manager(config_mock):
    with patch("pymysql.connect") as mock_connect:
        mock_connect.return_value = Mock()
        manager = MysqlManager(config_mock)
        manager.create_conx()
        yield manager


def test_create_connection_success(mysql_manager):
    assert mysql_manager.connection is not None


def test_create_database(mysql_manager):
    with patch.object(mysql_manager.connection, "cursor") as mock_cursor:
        mysql_manager.create_db()
        mock_cursor.assert_called()


def test_create_table(mysql_manager):
    with patch.object(mysql_manager.connection, "cursor") as mock_cursor:
        mysql_manager.create_table()
        mock_cursor.assert_called()


def test_setup_success(mysql_manager):
    with patch.object(mysql_manager, "create_db") as mock_create_db, patch.object(
        mysql_manager, "create_table"
    ) as mock_create_table:
        mysql_manager.setup()
        mock_create_db.assert_called()
        mock_create_table.assert_called()
