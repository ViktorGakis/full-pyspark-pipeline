# conftest.py
import os
import sys

from pytest import fixture

# Get the directory of the current script
current_script_dir = os.path.dirname(__file__)

# Get the parent directory of the current script's directory
parent_dir: str = os.path.dirname(current_script_dir)

# Append the parent directory to the sys.path
sys.path.append(parent_dir)


from Workflow.src.config import Config


@fixture(scope="module")
def config() -> Config:
    return Config()
