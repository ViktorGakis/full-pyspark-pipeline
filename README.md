## Containarization

We have containerized our development environment along with an mysql database server and phpadmin.

In order to create this dev environment please run

```bash
docker-compose up -d
```

Note that we have created a quite involved docker image `Dockerfile.pythondev` that includes all the needed groundwork for spark/pysparκ to be properly installed.

## Application File Tree

```graphql
Pipeline/
│
├── src/                          # Source directory for all submodules
│   │
│   ├── config.py                 # Configuration settings
│   │
│   ├── database/                 # Database management module
│   │   ├── __init__.py
│   │   └── manager.py            # DatabaseManager and MysqlManager classes
│   │
│   ├── spark/                    # Spark session management module
│   │   ├── __init__.py
│   │   └── session.py            # Spark class
│   │
│   ├── data_loading/             # Data loading module
│   │   ├── __init__.py
│   │   └── loader.py             # LoadTxtData class
│   │
│   ├── preprocessing/            # Data preprocessing module
│   │   ├── __init__.py
│   │   └── preprocessor.py       # PreprocessData class
│   │
│   ├── calculations/             # Calculations module
│   │   ├── __init__.py
│   │   └── calculators.py        # Calculation classes
│   │
│   ├── final_values/             # Final value calculation module
│   │   ├── __init__.py
│   │   └── finalizer.py          # FinalValues class
│   │
│   └── pipeline.py               # Pipeline class
│
└── __main__.py                   # Main application entry point as a package
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
