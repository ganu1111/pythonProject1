from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext


data = [1,2,3,4,5]
drdd = sc.parallelize(data)

for i in drdd.collect():
    print(i)