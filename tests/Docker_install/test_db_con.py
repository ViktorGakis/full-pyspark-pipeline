from Workflow.src.config import Config

USER: str = Config.get_env("MYSQL_ROOT_USER")
PASSWORD: str = Config.get_env("MYSQL_ROOT_PASSWORD")
HOST: str = Config.get_env("HOST")
PORT: str = int(Config.get_env("MYSQL_DOCKER_PORT"))
DATABASE: str = Config.get_env("MYSQL_DATABASE")


def test_mysql_conx(
    user=USER, password=PASSWORD, host=HOST, port=PORT, database=DATABASE
) -> None:
    import pymysql

    # Connect to MySQL
    try:
        connection = pymysql.connect(
            host=host, port=port, user=user, password=password, database=database
        )
    except Exception as e:
        print(e)
    else:
        assert True == True
    connection.close()
