from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data = "D:\\bigdata\\datasets\\asl.csv"
sc=spark.sparkContext
sc.setLogLevel("ERROR")
aslrdd=sc.textFile(data)

res = aslrdd.filter(lambda x: "hyd" in x)

for i in res.collect():
    print(i)