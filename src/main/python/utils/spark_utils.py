from pyspark.sql import SparkSession

def spark_session():
    return SparkSession.builder.getOrCreate()

