from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data="D:\\bigdata\\datasets\\bank-full.csv"
#df=spark.read.format("csv").option(";").option("header","true").load(data)
df=spark.read.format("csv").option("header","true").option("sep",";").option("inferSchema","true").load(data)
#df.show(5)
#df.printSchema()
#res=df.where(col("age")>90)
#res.show()
df.createOrReplaceTempView("practice")
res=spark.sql("select * from practice where age>90")
res.show()