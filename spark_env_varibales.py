import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['JAVA_HOME']="C:\\Program Files\\Java\\jdk-17"
os.environ['HADOOP_HOME']="C:\\Users\\Navya\\bigdata_setup\\hadoop"
os.environ['SPARK_HOME'] = "C:\\Users\\Navya\\bigdata_setup\\spark-3.5.4"
os.environ['PATH'] = ";".join([os.environ['PYSPARK_PYTHON'], os.environ['PYSPARK_DRIVER_PYTHON'],
            os.environ['JAVA_HOME']+"\\bin", os.environ['HADOOP_HOME']+"\\bin", 
            os.environ['SPARK_HOME']+"\\bin", os.environ['PATH']
        ])
from pyspark.sql import SparkSession
from datetime import datetime, date

import pandas as pd
from pyspark.sql import Row

spark = SparkSession.builder\
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

df = spark.createDataFrame([
    Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
    Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
    Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
])
print(df)
print(df.count())