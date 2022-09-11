from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data="D:\\bigdata\\datasets\\donations.csv"
df=spark.read.format("csv").option("header","true").load(data)
df.show()