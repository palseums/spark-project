from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id
from pyspark.sql.types import StructType, DateType, StringType, IntegerType, StructField

from lib.logger import Log4j

conf = SparkConf()
conf.set("spark.app.name", "palani spark application")
spark = SparkSession.builder.config(conf=conf).getOrCreate()
conf = spark.sparkContext.getConf()
var1 = conf.get("spark.app.name")
print("Application name is " + var1)
logger = Log4j(spark)

# StructType = Represents Data Frame ROW Structure
# StructField = Represents Data Frame Column Structure
# First way of defining the schema
flightSchemaStruct = StructType([
    StructField("Name", StringType()),
    StructField("Age", IntegerType()),
    StructField("salary", IntegerType()),
    StructField("Date", DateType()),
    StructField("country", StringType()),
    StructField("group", IntegerType()),
])

flightTimeCsvDF = spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(flightSchemaStruct) \
    .option("mode", "FAILFAST") \
    .option("dateFormat", "M/d/y") \
    .load("data/data*.csv")

flightTimeCsvDF.show(5)
# In the below line of code converting the DF to rdd and getting the number of partition
logger.info("Num of Partitions before " + str(flightTimeCsvDF.rdd.getNumPartitions()))
# In the below line of code we are getting the number of records within the partitions
flightTimeCsvDF.groupBy(spark_partition_id()).count().show()

# We are going to force the partition to a specified count
#partitionedDF = flightTimeCsvDF.repartition(4)
#logger.info("Num of Partitions After " + str(partitionedDF.rdd.getNumPartitions()))
#partitionedDF.groupBy(spark_partition_id()).count().show()
# partitionedDF.write\
#     .format("csv")\
#     .mode("overwrite")\
#     .option("path", "output/csv/")\
#     .partitionBy("country", "group")\
#     .save()
flightTimeCsvDF.write\
    .format("csv")\
    .mode("overwrite")\
    .option("path", "output/csv/")\
    .partitionBy("country", "group")\
    .option("maxRecordsPerFile", 1)\
    .save()

