import os
import sys
from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row
#os.environ["PYSPARK_PYTHON"] = sys.executable
from pyspark.sql import SparkSession
try:
    # spark = SparkSession.builder.appName("FixCrashedWorker")\
    # .config("spark.driver.memory", "8g")\
    # .config("spark.executor.memory", "4g")\
    # .config("spark.executor.cores", "4")\
    # .config("spark.driver.maxResultSize", "2g")\
    # .getOrCreate()
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
#     df = spark.createDataFrame([
#     Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
#     Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
#     Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
# ])
    #df = spark.read.csv("data\salary_50L.csv", header=True)
    pandas_df = pd.DataFrame({
    'a': [1, 2, 3],
    'b': [2., 3., 4.],
    'c': ['string1', 'string2', 'string3'],
    'd': [date(2000, 1, 1), date(2000, 2, 1), date(2000, 3, 1)],
    'e': [datetime(2000, 1, 1, 12, 0), datetime(2000, 1, 2, 12, 0), datetime(2000, 1, 3, 12, 0)]
})
    df = spark.createDataFrame(pandas_df)
    print(df)
    print(df.count())
except Exception as err:
    print(err)
finally:
    spark.stop()