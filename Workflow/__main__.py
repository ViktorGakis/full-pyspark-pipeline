from src.config import Config

# from src.pipeline import Pipeline
# from src.database.query_service import DatabaseQueryService


def main():
    config = Config()
    # db_query_service = DatabaseQueryService()

    # pipeline = Pipeline(config, db_query_service)
    # pipeline.execute()

    # You can also handle outputs, logging, or any post-pipeline operations here
    print(f'{config.get_env("MYSQL_ROOT_USER")}')


if __name__ == "__main__":
    main()
