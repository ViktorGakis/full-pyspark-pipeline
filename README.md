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
    @staticmethod
    def get_config(key: str, default=None):
        return os.getenv(key, default)
```

This class serves as a configuration holder. It follows the SRP as it's only responsible for holding configuration settings.

It is also quite dynamic since it reads directly from the environment variables.

#### MYSQL classes

We use the Dependency Inversion Principle (DIP) so that there is depence on abstractions not not conrections. i.e not directly implementing the actions.

```py
# Workflow/src/database/manager.py

from abc import ABC, abstractmethod

class DatabaseManager(ABC):
    @abstractmethod
    def create_db(self, *args, **kwargs):
        pass

class MysqlManager(DatabaseManager):
    def create_db(self, *args, **kwargs):
        # Implementation for creating MySQL database
        pass
```

We use the Single Responsibility Principle (SRP), as the DatabaseQueryService will act as a service. This service will encapsulate the logic for querying the database.

```py
# Workflow/src/database/query_service.py

class DatabaseQueryService:
    def handle_query(self, *args, **kwargs):
        """Function to query a specific instrument in the database."""
        pass

    def query_db_closure(self, *args, **kwargs):
        """Generates a closure function for querying the database."""
        pass
```
