from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id
from pyspark.sql.types import StructType, DateType, StringType, IntegerType, StructField

from lib.logger import Log4j

conf = SparkConf()
conf.set("spark.app.name", "palani spark application")
# In this spark session we are getting the hive support to

spark = SparkSession \
    .builder \
    .master("local[3]") \
    .appName("SparkSQLTableDemo") \
    .enableHiveSupport() \
    .getOrCreate()

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

spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB1")
spark.catalog.setCurrentDatabase("AIRLINE_DB1")

flightTimeCsvDF.write\
    .format("csv")\
    .mode("overwrite")\
    .bucketBy(2, "country", "group")\
    .saveAsTable("table1")
logger.info(spark.catalog.listTables("AIRLINE_DB1"))
