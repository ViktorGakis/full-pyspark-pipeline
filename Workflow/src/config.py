from os import getenv

from dotenv import load_dotenv

load_dotenv()


class Config:
    @staticmethod
    def get_config(key: str, default=None):
        return getenv(key, default)
