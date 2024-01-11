# conftest.py
import os
import sys

from pytest import fixture

sys.path.append(os.path.dirname(__file__))

from Workflow.src.config import Config


@fixture(scope="module")
def config() -> Config:
    return Config()
