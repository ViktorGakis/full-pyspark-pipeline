def test_mysql_conx(config) -> None:
    import pymysql

    # Connect to MySQL
    try:
        connection = pymysql.connect(
            host=config.HOST,
            port=int(config.MYSQL_DOCKER_PORT),
            user=config.MYSQL_ROOT_USER,
            password=config.MYSQL_ROOT_PASSWORD,
            database=config.MYSQL_DATABASE,
        )
    except Exception as e:
        print(e)
    else:
        assert True == True
    connection.close()
