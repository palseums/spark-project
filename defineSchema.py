#from pyspark.sql import *
from pyspark import SparkConf
from pyspark.sql import SparkSession
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

])

# Second way of defining the schema

flightSchemaDDL = """FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT """


# This is for loading CSV file
# Here we are defining the schema via schema option and mode option also we are setting like FAILFAST
flightTimeCsvDF = spark.read\
        .format("csv")\
        .option("header", "true")\
        .schema(flightSchemaStruct)\
        .option("mode", "FAILFAST")\
        .option("dateFormat", "M/d/y")\
        .load("data/data*.csv")

flightTimeCsvDF.show(5)
logger.info("CSV Schema" + flightTimeCsvDF.schema.simpleString())

# This is for loading JSON file
# There is no header for JSON
flightTimeJsonDF = spark.read\
    .format("json")\
    .schema(flightSchemaDDL)\
    .option("dateFormat", "M/d/y")\
    .load("data/flight*.json")
flightTimeJsonDF.show(5)
logger.info("JSON Schema" + flightTimeJsonDF.schema.simpleString())


# This is for loading Parquet File
flightTimeParquetDF = spark.read\
    .format("parquet")\
    .load("data/flight*.parquet")
flightTimeParquetDF.show(5)
logger.info("Parquet Schema" + flightTimeParquetDF.schema.simpleString())







