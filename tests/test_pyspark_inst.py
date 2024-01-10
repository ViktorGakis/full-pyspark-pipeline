import findspark
findspark.init()

from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Initialize SparkSession
    spark = SparkSession.builder.appName("PySparkExample").getOrCreate()

    # Create a DataFrame with a single column "value"
    data = [("Hello",), ("World",)]
    columns = ["value"]
    df = spark.createDataFrame(data, columns)

    # Show the DataFrame
    df.show()

    # Stop the SparkSession
    spark.stop()