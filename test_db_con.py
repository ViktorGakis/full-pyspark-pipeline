from os import getenv

from dotenv import load_dotenv

load_dotenv()

USER: str = getenv("MYSQL_ROOT_USER")
PASSWORD: str = getenv("MYSQL_ROOT_PASSWORD")
HOST: str = getenv("HOST")
PORT: str = int(getenv("MYSQL_DOCKER_PORT"))
DATABASE: str = getenv("MYSQL_DATABASE")


def test_mysql_conx() -> None:
    import pymysql

    # Connect to MySQL
    try:
        connection = pymysql.connect(
            host=HOST,
            port=PORT,
            user=USER,
            password=PASSWORD,
            # database=DATABASE
        )
    except Exception as e:
        print(e)
    else:
        print("Connection Success")
    connection.close()


if __name__ == "__main__":
    test_mysql_conx()