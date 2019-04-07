from pyspark.sql import SparkSession

def get_instance():

    __spark_instance = SparkSession.builder.getOrCreate()

    return __spark_instance