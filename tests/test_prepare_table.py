from os import getenv

import pymysql
from dotenv import load_dotenv

load_dotenv()

USER: str = getenv("MYSQL_ROOT_USER")
PASSWORD: str = getenv("MYSQL_ROOT_PASSWORD")
HOST: str = getenv("HOST")
PORT: str = int(getenv("MYSQL_DOCKER_PORT"))
DATABASE: str = getenv("MYSQL_DATABASE")


def create_con():
    return pymysql.connect(
        host=HOST, port=PORT, user=USER, password=PASSWORD, database=DATABASE
    )  # type: ignore


def create_table(conx):
    # Create a cursor object to execute SQL queries
    cursor = conx.cursor()

    # Define the table creation query
    create_table_query = """
    CREATE TABLE test_table (
        id INT PRIMARY KEY,
        name VARCHAR(50)
    );
    """

    try:
        # Execute the table creation query
        cursor.execute(create_table_query)

        # Commit the changes
        conx.commit()

        print("Table 'test_table' created successfully.")

    finally:
        # Close the cursor and connection
        cursor.close()


def insert_values(conx):
    # Create a cursor object to execute SQL queries
    cursor = conx.cursor()

    # Insert sample data into the table
    insert_data_query = """
    INSERT INTO test_table (id, name) VALUES
        (1, 'John'),
        (2, 'Alice'),
        (3, 'Bob');
    """

    try:
        # Execute the data insertion query
        cursor.execute(insert_data_query)

        # Commit the changes
        conx.commit()

        print("Sample data inserted successfully.")

    finally:
        # Close the cursor and connection
        cursor.close()


def table_preparation():
    conx = create_con()
    drop_table(conx)
    create_table(conx)
    insert_values(conx)
    conx.close()


def drop_table(conx=None):
    close_conx = False
    if conx is None:
        conx = create_con()
        close_conx = True

    # Create a cursor object to execute SQL queries
    cursor = conx.cursor()

    # Define the table deletion query
    drop_table_query = "DROP TABLE IF EXISTS test_table;"

    try:
        # Execute the table deletion query
        cursor.execute(drop_table_query)

        # Commit the changes
        conx.commit()

        print("Table 'test_table' deleted successfully.")

    finally:
        # Close the cursor and connection
        cursor.close()
        if close_conx:
            conx.close()
