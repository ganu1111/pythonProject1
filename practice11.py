from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

data = "D:\\bigdata\\datasets\\asl.csv"
#aslrdd = spark.sparkContext.textFile()

aslrdd = sc.textFile(data)

#res=aslrdd.filter(lambda x: "hyd" in x)

for i in aslrdd.collect():
    print(i)