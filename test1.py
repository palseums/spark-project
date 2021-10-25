from pyspark import SparkConf
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

from lib.logger import Log4j


def to_date_df(df, fmt, fld):
    return df.withColumn(fld, to_date(fld, fmt))

conf = SparkConf()
conf.set("spark.app.name", "palani spark application")
spark = SparkSession.builder.config(conf=conf).getOrCreate()
conf = spark.sparkContext.getConf()
var1 = conf.get("spark.app.name")
print("Application name is " + var1)
logger = Log4j(spark)

my_schema = StructType([
    StructField("ID", StringType()),
    StructField("EventDate", StringType())])

my_rows = [Row("123", "04/05/2020"), Row("124", "04/05/2020"), Row("456", "04/05/2020"), Row("789", "04/05/2020")]
# Creating an RDD over here
my_rdd = spark.sparkContext.parallelize(my_rows)
# Creating a DF from RDD and setting the schema for the DF
my_df = spark.createDataFrame(my_rdd, my_schema)
my_df.printSchema()
my_df.show()
print("After the change")
new_df = to_date_df(my_df, "M/d/y", "EventDate")
new_df.printSchema()
new_df.show()
#new_df.withColumn("group", col("2"))
new_df.show()
row1 = new_df.take(1)
print(row1)
new1_df = new_df.toDF('f1', 'f2')
print(new1_df.collect())
# where and filter functions are same. Here it is checking for a condition where id = 123. Here id is a column
new_df.where("ID = 123").groupBy("ID").count().show()


