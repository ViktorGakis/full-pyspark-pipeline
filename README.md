## Building

We have containerized our development environment along with an mysql database server and phpadmin.

In order to create this dev environment please run

```bash
git clone https://github.com/ViktorGakis/wipro-python-app-prog-coding-challenge.git
cd wipro-python-app-prog-coding-challenge
docker-compose up -d
```

Note that we have created a quite involved docker image `Dockerfile.pythondev` that includes all the needed groundwork for spark/pysparκ to be properly installed.

## Application File Tree

```graphql
Workflow/
├── __init__.py                     # Makes Workflow a Python package
├── __main__.py                     # Entry point of the application
└── src                             # Source directory for the application modules
    ├── __init__.py                 # Makes src a Python package
    ├── calculation_engine          # Module for calculation logic
    │   ├── __init__.py             
    │   └── calculators.py          
    ├── config.py                   # Configuration settings and parameters
    ├── data_loading                # Module for loading and summarizing data
    │   ├── __init__.py             
    │   ├── data_summary.py         
    │   ├── loader.py               
    │   └── schema_provider.py      
    ├── data_preprocessing          # Module for preprocessing the data
    │   ├── __init__.py             
    │   └── preprocessor.py         
    ├── database                    # Module for database interactions
    │   ├── __init__.py             
    │   ├── database_injector.py    
    │   ├── manager.py              
    │   ├── mysql_manager.py        
    │   ├── query_service.py        
    │   └── schema_provider.py      
    ├── final_values                # Module for final value calculations
    │   ├── __init__.py             
    │   └── finalizer.py            
    ├── pipeline.py                 # Coordinates the overall data processing pipeline
    └── spark                       # Module for Spark session management
        ├── __init__.py             
        └── session.py              

```

The Workflow package can be run as

```bash
# run with python
$ python Workflow/

# run in a spark cluster locally
$ spark-submit --master local[*] --deploy-mode client /app/Workflow/__main__.py
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

#### data_loading package

We use the Single Responsibility Principle (SRP) as these classes should responsible for loading the text data, creating the appropriate scheme and summarizing printing a summary and the head of the loaded df.

```py
# Workflow/src/data_loading/loader.py

class LoadTxtData:
    def __init__(self, spark):
        self.spark = spark

    def load_source_file(self, *args, **kwargs):
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

### data_preprocessing package

We use the Single Responsibility Principle (SRP) as this class should responsible for preprocessing the loaded data.

In fact the idea of the SRP propagates to the methods themselves since the perform a single action too.

We used static methods since these methods operate on the data passed to them and do not need to maintain any internal state. They provide utility functions that transform a DataFrame and return a new DataFrame.

```py
# Workflow/src/preprocessing/preprocessor.py

class PreprocessData:
    @staticmethod 
    def date_transform(self, *args, **kwargs):
        pass

    @staticmethod 
    def date_sorting(self, *args, **kwargs):
        pass

    @staticmethod 
    def business_date_validation(self, *args, **kwargs):
        pass

    @staticmethod 
    def cutoff_after_current_date(self, *args, **kwargs):
        pass
```

### CalculationEngine package

We follow the exact same structure as the data_preprocessing package.

```py
# Workflow/src/calculation_engine/calculators.py

class CalculationEngine:
    """Class for performing various calculations on financial instruments."""

    @staticmethod
    def instr_1_mean(*args,**kwargs):
        """Calculate the mean for INSTRUMENT1."""

    @staticmethod
    def instr_2_mean_nov_2014(*args, **kwargs):
        """Calculate the mean for INSTRUMENT2 for November 2014."""

    @staticmethod
    def instr_3_statistics(*args, **kwargs):
        """Perform statistical on-the-fly calculations for INSTRUMENT3."""

    @staticmethod
    def sum_newest_10_elems(*args, **kwargs):
        """Calculate the sum of the newest 10 elements in terms of the date."""

    @staticmethod
    def run(*args, **kwargs) -> None:
        CalculationEngine.instr_1_mean(*args, **kwargs)
        CalculationEngine.instr_2_mean_nov_2014(*args, **kwargs)
        CalculationEngine.instr_3_statistics(*args, **kwargs)
        CalculationEngine.sum_newest_10_elems(*args, **kwargs)
```

### database package

We use the Dependency Inversion Principle (DIP) so that there is dependence on abstractions not conrections. i.e not directly implementing the actions.

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

class DatabaseService:
    def get_multipliers_df(self, *args, **kwargs):
        """Function to query a specific instrument in the database."""
        pass
```

```py
# Workflow/src/database/schema_provider.py

class DBSchemaProvider:
     schema = StructType(...)
```

```py
# Workflow/src/database/mysql_manager.py

class MysqlManager(DatabaseManager):
    """A class that handles direct mysql connection and operations"""
    def __init__(self, config) -> None:
        super().__init__()
        self.config = config
        self.connection = None

    def create_conx(self) -> None:
        """Connection factory"""

    def close_conx(self) -> None:
        """Close connection"""

    def create_db(self) -> None:
        """Create a database if it does not exist."""

    def create_table(self) -> None:
        """Create a table if it does not exist."""

    def setup(self) -> None:
        """Set up the database and table."""
```

```py
# Workflow/src/database/database_injector.py

class DatabaseInjector:
    """Injects data directly into the table INSTRUMENT_PRICE_MODIFIER"""
    def __init__(self, spark: SparkSession, config):
        self.spark: SparkSession = spark
        self.config = config

    def inject_data(self, data, schema, table_name):
        # Create DataFrame from data
        # Write DataFrame to the specified database table
```

Finally, we added the cache mechanism that deals with the database frequency update. We decided to use a closure, in the form of a decorator, to be applied on our get_multipliers_df from DatabaseService.

```py
# Workflow/src/database/cache_mechanism.py

from datetime import datetime, timezone
from typing import Optional, Union

from pyspark.sql import DataFrame


def cache_query(seconds=5):  # -> Callable[..., Callable[..., DataFrame]]:
    def decorator(fetch_function):  # -> Callable[..., DataFrame]:
        cache: dict[str, Union[Optional[datetime], Optional[DataFrame]]] = {
            "last_updated": None,
            "data": None,
        }

        def wrapper(*args, **kwargs) -> DataFrame:
            current_time: datetime = datetime.now(timezone.utc)
            if (
                cache["last_updated"] is None
                or (current_time - cache["last_updated"]).total_seconds() > seconds  # type: ignore
            ):
                if cache["data"] is not None:
                    # Clear previous cache
                    cache["data"].unpersist()  # type: ignore 
                cache["data"] = fetch_function(*args, **kwargs)
                # Cache the new DataFrame
                cache["data"].cache()  
                cache["last_updated"] = current_time
            return cache["data"]  # type: ignore

        return wrapper

    return decorator
```

The data is cached and uncached appropriately, as per Spark's best practices for memory management, especially in big data scenarios.

- .cache() is used to cache the dataframe
- .unpersist() is used to clean up the previous cache

This decorator should effectively cache the results of the fetch_function and only refresh the cache if more than seconds (5 seconds in this case) have elapsed since the last update.

#### FinalValues class

We use Dependency Inversion Principle (DIP), as the FinalValues class is directly handling database queries and is always better to depend on an abstraction rather than an actual implementation(i,e than the concrete details of database querying).

```py
# Workflow/src/final_values/finalizer.py

class FinalValues:
    def __init__(self, multipliers_df):
        self.multipliers_df = multipliers_df

    def final_values_cal(self, *args, **kwargs):
        pass
```

The explicit form of this class is

```py
class FinalValues:
    def __init__(self, multipliers_df):
        self.multipliers_df = multipliers_df

    def final_values_cal(self, df):
        # Join the input DataFrame with the multipliers DataFrame
        df_with_multipliers = df.join(
            self.multipliers_df, on="INSTRUMENT_NAME", how="left"
        )

        # Calculate the final value
        return df_with_multipliers.withColumn(
            "FINAL_VALUE", F.col("VALUE") * F.coalesce(F.col("MULTIPLIER"), F.lit(1))
        )
```

The method final_values_cal performs:

- First a left join on the INSTRUMENT_NAME with the multiplier_df.
- Then the actual calculation of the final values takes place and the column is named FINAL_VALUE

The left join ensures that all rows from the left DataFrame (df) are retained, and if there is no matching row in multipliers_df, the columns from multipliers_df will contain null values.

Then we multiply the VALUE col with the MULTIPLIER col `F.col("VALUE") * F.coalesce(F.col("MULTIPLIER"), F.lit(1))`. To handle the MULTIPLIER column we use the coalesce function which replaces the null values with 1 (from F.lit(1)) and extracts the non-null values accordingly.

If the INSTRUMENT_PRICE_MODIFIER table has more than one multiplier for an instrument name, then the join will add sequentially all the multipliers on the current instances of the instrument in .TXT data and repeat until all instances have a multiplier.

### Pipeline

The Pipeline class practically streamlines the whole process by providing encapsulation

- This hides the internal state and functionality from outside interference and misuse.

Single Responsibility Principle (SRP): Every method in the class has a single responsibility. For example, load_data is only responsible for loading data. This makes the class more robust, easier to maintain, and conducive to unit testing.

Abstraction: The DataPipeline provides an abstract interface to a set of operations, hiding the complex underlying logic of data processing steps from the user of the pipeline.

Modularity: The pipeline’s structure allows for easy modification and extension of individual parts without affecting the whole.

- New steps can be added or existing ones modified with minimal impact on other parts of the pipeline.

Reusability: The pipeline design facilitates the reuse of common processes or methods in different contexts within the application, thus promoting code reusability.

```py
# Workflow/src/pipeline.py

class Pipeline:
    def __init__(self, config, verbose=False):
        self.config = config
        self.verbose = verbose
        self.spark = Spark(config).create()

    def log(self, message):
        """handles logging"""

    def load_data(self):
        """Handles loading data"""

    def preprocess_data(self, df_txt):
        """Handles preprocessing data"""

    def run_calculations(self, df_processed):
        """Handles running the calculations"""

    def setup_database(self):
        """Handles setting up the database"""

    def inject_data(self, data, schema):
        """Handles injecting data"""

    @cache_query(seconds=5) # cache every 5 seconds
    def fetch_multipliers(self):
        """Handles fetching the multipliers"""

    def calculate_final_values(self, df_processed, multipliers_df):
        """Handles calculating the final values"""

    def run_pipeline(self, data=None):
        """Handles runnung the whole pipeline"""
```

Notice the method cache_query is applied on fetch_multiplier

```py
    @cache_query(seconds=5) # cache every 5 seconds
    def fetch_multipliers(self):
        """Handles fetching the multipliers"""
```

## Tests

We have create two sets of tests in the tests folder.

```py
tests
├── Docker_install                  # ensures the docker containers have been properly run
│   ├── test_db_con.py              # tests mysql database connection
│   ├── test_prepare_table.py       # tests mysql database operations
│   ├── test_pyspark_db_con.py      # tests the pyspark mysql database connection
│   └── test_pyspark_inst.py        # tests the pyspark/spark installation
│
└── Workflow                        # ensures proper functionality of the tasks
    ├── conftest.py                 # various test dependencies
    ├── test_cached_multipliers.py  # test the query cache mechanism    
    ├── test_config.py              # tests the config
    ├── test_data_loading.py        # tests the loadinbg of data
    ├── test_data_preprocess.py     # tests the preprocess of data
    ├── test_final_values.py        # tests the calculation of the final values
    ├── test_mysqlmanager.py        # tests the mysql database connection with pymysql
    └── test_pyspark_db_con_work.py # tests the mysql database connection with spark
```

currently the test coverage is 96% which is good enough given that we have no actual big data.

![Alt text](./tests/tests.png)

You can see a test coverage report by running

```bash
pytest --cov=Workflow
```

or just check which tests pass or fail using

```bash
pytest tests
```

When you run docker-compose all the tests should pass otherwise something is wrong with the building process.

## Notes

- The notebooks task_part_1.ipynb and task_part_2.ipynb served as the foundation and mostly testing of the Workflow package. Since then a lot of changes have happened thus they are now only an approximation.

- The .jar file was included for completeness.

- The .env would normally not be included in production, but this is local development and quite convenient for reproducing the results.

- We have also included the proper ports to access the Spark UI in <http://localhost:51000>

- You can also access the phpadmin on [http://localhost:](http://localhost:9050/) with username:root and password: example
