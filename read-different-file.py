#from pyspark.sql import *
from pyspark import SparkConf
from pyspark.sql import SparkSession
from lib.logger import Log4j
conf = SparkConf()
conf.set("spark.app.name", "palani spark application")
spark = SparkSession.builder.config(conf=conf).getOrCreate()
conf = spark.sparkContext.getConf()
var1 = conf.get("spark.app.name")
print("Application name is " + var1)
logger = Log4j(spark)
# This is for loading CSV file
flightTimeCsvDF = spark.read\
                .format("csv")\
                .option("header", "true")\
                .option("inferSchema", "true")\
                .load("data/flight*.csv")
flightTimeCsvDF.show(5)
logger.info("CSV Schema" + flightTimeCsvDF.schema.simpleString())

# This is for loading JSON file
# There is no header for JSON
flightTimeJsonDF = spark.read\
                .format("json")\
                .load("data/flight*.json")
flightTimeJsonDF.show(5)
logger.info("JSON Schema" + flightTimeJsonDF.schema.simpleString())
# This is for loading Parquet File
flightTimeParquetDF = spark.read\
    .format("parquet")\
    .load("data/flight*.parquet")
flightTimeParquetDF.show(5)
logger.info("Parquet Schema" + flightTimeParquetDF.schema.simpleString())







