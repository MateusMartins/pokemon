from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import col, expr, when
from pyspark import SparkContext, SparkConf
from globals import spark
import json

sc = SparkContext()
sqlContext = SQLContext(sc)
spark_instance = spark.get_instance()

def open_csv(name):
    
    df = spark_instance.read.csv(
        path = "./data/" + name + ".csv", 
        sep = ",",
        header = True,
        schema = get_schema(name)
    )

    return df

def get_schema(schema_name):
    
    schema_path = './schema/'
    
    with open(schema_path + schema_name + ".json", 'r') as file:
        data = json.load(file)

        params = []
        for _ in range(len(data["columns"])):
            if data["columns"][_]["column_type"] == "int":
                params.append(StructField(data["columns"][_]["name"].lower(), IntegerType(), data["columns"][_]["null"]))
            elif data["columns"][_]["column_type"] == "float":
                params.append(StructField(data["columns"][_]["name"].lower(), DoubleType(), data["columns"][_]["null"]))
            elif data["columns"][_]["column_type"] == "str":
                params.append(StructField(data["columns"][_]["name"].lower(), StringType(), data["columns"][_]["null"]))
            elif data["columns"][_]["column_type"] == "bool":
                params.append(StructField(data["columns"][_]["name"].lower(), BooleanType(), data["columns"][_]["null"]))

    return StructType(params)

def verify_integrity(df_main, df_other, key=["name"]):

    df_ok = df_main.join(df_other, key, 'inner')
    df_nok_main = df_main.join(df_other, key, 'leftanti')

    return df_ok, df_nok_main


def change_columns(df, name):
    df = df.withColumn(name, ((col(name) + col(name+'_1'))/2))
    df = df.drop(name+'_1')
    
    return df

def adjust_total(df):
    df = df.withColumn('total', ((col('speed') + col('attack') + col('sp_def') + col('sp_atk') + col('defense') + col('hp'))))
    df = df.drop('base_total')
    return df

def drop_columns(df):
    df = df.drop('Type 1', 'Type 2', 'legendary')
    
    return df
