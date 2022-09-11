from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local").appName("testing").getOrCreate()
host="jdbc:mysql://sravanthidb.c7nqndsntouw.us-east-1.rds.amazonaws.com:3306/sravanthidb?useSSL=false"
df=spark.read.format("jdbc").option("url",host).option("user","myuser").option("password","mypassword")\
    .option("dbtable","emp").option("driver","com.mysql.jdbc.Driver").load()
#df.show()
#process data
res=df.na.fill(0,['comm','mgr']).withColumn("comm", col("comm").cast(IntegerType()))\
    .withColumn("hiredate", date_format(col("hiredate"),"yyyy/MMM/dd"))

res.write.mode("overwrite").format("jdbc").option("url",host).option("user","myuser").option("password","mypassword")\
    .option("dbtable","empclean").option("driver","com.mysql.jdbc.Driver").save()



res.show()
res.printSchema()
 #: java.lang.ClassNotFoundException: com.mysql.jdbc.Driver
#mysql dependency problem so pls add mysql jar and place in spark/jars folder
