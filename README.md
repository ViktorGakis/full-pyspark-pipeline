## Containarization

We have containerized our development environment along with an mysql database server and phpadmin.

In order to create this dev environment please run

```bash
docker-compose up -d
```

Note that we have created a quite involved docker image `Dockerfile.pythondev` that includes all the needed groundwork for spark/pysparκ to be properly installed.

## Application File Tree

```graphql
Workflow/
│
├── src/
│   ├── __init__.py                   # Make src a Python package
│   │
│   ├── config.py                     # Configuration settings
│   │
│   ├── database/                     # Database management module
│   │   ├── __init__.py               # Make database a Python package
│   │   ├── manager.py                # DatabaseManager and MysqlManager classes
│   │   └── query_service.py          # DatabaseQueryService class
│   │
│   ├── spark/                        # Spark session management module
│   │   ├── __init__.py               # Make spark a Python package
│   │   └── session.py                # Spark class
│   │
│   ├── data_loading/                 # Data loading module
│   │   ├── __init__.py               # Make data_loading a Python package
│   │   └── loader.py                 # LoadTxtData class
│   │
│   ├── preprocessing/                # Data preprocessing module
│   │   ├── __init__.py               # Make preprocessing a Python package
│   │   └── preprocessor.py           # PreprocessData class
│   │
│   ├── calculations/                 # Calculations module
│   │   ├── __init__.py               # Make calculations a Python package
│   │   └── calculators.py            # CalculationEngine class
│   │
│   ├── final_values/                 # Final value calculation module
│   │   ├── __init__.py               # Make final_values a Python package
│   │   └── finalizer.py              # FinalValues class
│   │
│   └── pipeline.py           # DataProcessingPipeline or WorkflowManager class
│
├── __init__.py                       # Make PipelineProject a Python package
│
└── __main__.py                       # Main application entry point as a package

```

the pipeline package is supposed to be runnable directly with python but with spark job as a job

```bash
# run with python
$ python -m Pipeline

# run in a spark cluster locally
$ spark-submit --master local[*] --deploy-mode client /app/pipeline/app.py
```

the general command to run as a spark job is

```bash
spark-submit --master [master-url] --deploy-mode [deploy-mode] path/to/your_project_name
```

### Master URL

The master-url specifies the master node of the Spark cluster. It tells Spark how to connect to a cluster manager which allocates resources for your application. Here are some common examples:

**Local Mode**:

- --master local - Runs Spark locally with one worker thread (i.e., no parallelism).

- --master local[*] - Runs Spark locally with as many worker threads as logical cores on your machine.

**Standalone Cluster:**

- --master spark://HOST:PORT - Connects to a Spark standalone cluster manager running at HOST:PORT.

**YARN Cluster:**

--master yarn - Connects to a YARN cluster. Resource allocation will be handled by YARN.

**Mesos Cluster:**

- --master mesos://HOST:PORT - Connects to a Mesos cluster.

**Kubernetes Cluster:**

- --master k8s://<https://HOST:PORT> - Runs on a Kubernetes cluster.

### Deploy Mode

The deploy-mode specifies where the driver program runs.

**Client Mode**

(--deploy-mode client): The driver runs on the machine where the spark-submit command is executed. This is often used for interactive and debugging purposes.

**Cluster Mode**

(--deploy-mode cluster): The driver runs on a node in the cluster. This is common in production, as it allows the driver to be managed by the cluster manager (like YARN or Mesos).

## Explanation of the OOP structure

### Review of SOLID principles

#### 1. Single Responsibility Principle (SRP)

Each class should have only one reason to change, meaning it should have only one job or responsibility.

#### 2. Open/Closed Principle (OCP)

Classes should be open for extension but closed for modification.

#### 3. Liskov Substitution Principle (LSP)

Objects of a superclass should be replaceable with objects of its subclasses without affecting the correctness of the program.

#### 4. Interface Segregation Principle (ISP)

Larger interfaces should be split into smaller ones. By doing so, a class will only have to know about the methods that are of interest to it.

#### 5. Dependency Inversion Principle (DIP)

High-level modules should not depend on low-level modules. Both should depend on abstractions.

### Class explanations

Our classes are built mostly with the SOLID principles in mind

#### Config class

```python
# Workflow/src/config.py
from os import getenv
from dotenv import load_dotenv

load_dotenv()

class Config:
    ...
    @staticmethod
    def get_config(key: str, default=None):
        return os.getenv(key, default)
```

This class serves as a configuration holder. It follows the SRP as it's only responsible for holding configuration settings.

It is also quite dynamic since it reads directly from the environment variables.

thus the main is modified as

```py
# Workflow/__main__.py
from .src import Config

config = Config()


def main():
    config = Config()

if __name__ == "__main__":
    main()
```

#### spark package

We use the Single Responsibility Principle (SRP) as this class should focus solely on Spark session management.

```py
# Workflow/src/spark/session.py
class Spark:
    def __init__(self, config: Config):

    def create(self, *args, **kwargs):
        # creates a spark session based on the config
        pass
```

thus the main is modified as

```py
# Workflow/__main__.py
from pyspark.sql import DataFrame, SparkSession

from .src import Config, Spark

config = Config()


def main() -> None:
    config = Config()
    spark: SparkSession = Spark(config).create()

if __name__ == "__main__":
    main()
```

#### data_loading package

We use the Single Responsibility Principle (SRP) as these classes should responsible for loading the text data, creating the appropriate scheme and summarizing printing a summary and the head of the loaded df.

```py
# Workflow/src/data_loading/loader.py

class LoadTxtData:
    def __init__(self, spark):
        self.spark = spark

    def load_source_file(self, *args, **kwargs):
        pass

    def summary(self, *args, **kwargs):
        pass
```

```py
# Workflow/src/data_loading/data_summary.py
class DataSummary:
    @staticmethod
    def display_summary(*args, **kwargs) -> None:
        pass
```

```py
# Workflow/src/data_loading/schema_provider.py
class TxtSchemaProvider:
    schema = StructType(...)
```

thus the main is modified as

```py
# Workflow/__main__.py
from pyspark.sql import DataFrame, SparkSession

from .src import Config, DataSummary, LoadTxtData, Spark, TxtSchemaProvider

config = Config()


def main() -> None:
    config = Config()
    spark: SparkSession = Spark(config).create()
    df_txt: DataFrame = LoadTxtData(
        spark, TxtSchemaProvider.schema, config.TXT_FILE_REL_PATH_STR  # type: ignore
    ).load_source_file()
    DataSummary.display_summary(df_txt)


if __name__ == "__main__":
    main()
```

### preprocess package

We use the Single Responsibility Principle (SRP) as this class should responsible for preprocessing the loaded data.

In fact the idea of the SRP propagates to the methods themselves since the perform a single action too.

We used static methods since these methods operate on the data passed to them and do not need to maintain any internal state. They provide utility functions that transform a DataFrame and return a new DataFrame.

```py
# Workflow/src/preprocessing/preprocessor.py

class PreprocessData:
    def __init__(self, data):
        self.data = data

    def date_transform(self, *args, **kwargs):
        pass

    def date_sorting(self, *args, **kwargs):
        pass

    def business_date_validation(self, *args, **kwargs):
        pass

    def cutoff_after_current_date(self, *args, **kwargs):
        pass
```
