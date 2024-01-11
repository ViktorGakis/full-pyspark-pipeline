def test_pyspark_con():
    import findspark

    findspark.init()

    from pyspark.sql import SparkSession

    # Initialize SparkSession
    spark = SparkSession.builder.appName("PySparkExample").getOrCreate()

    # Create a DataFrame with a single column "value"
    data = [("Hello",), ("World",)]
    columns = ["value"]
    df = spark.createDataFrame(data, columns)

    assert df.collect()[0]["value"] == "Hello"

    # Stop the SparkSession
    spark.stop()
