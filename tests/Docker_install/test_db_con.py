from os import getenv

from dotenv import load_dotenv

load_dotenv()

USER: str = getenv("MYSQL_ROOT_USER")
PASSWORD: str = getenv("MYSQL_ROOT_PASSWORD")
HOST: str = getenv("HOST")
PORT: str = int(getenv("MYSQL_DOCKER_PORT"))
DATABASE: str = getenv("MYSQL_DATABASE")


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
