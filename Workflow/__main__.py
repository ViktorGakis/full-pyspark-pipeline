from src.pipeline import Pipeline
from src.config import Config
from src.database.query_service import DatabaseQueryService

def main():
    config = Config()
    db_query_service = DatabaseQueryService()

    pipeline = Pipeline(config, db_query_service)
    pipeline.execute()

    # You can also handle outputs, logging, or any post-pipeline operations here

if __name__ == "__main__":
    main()
