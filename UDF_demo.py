from pyspark.sql import *
from pyspark import SparkConf
from pyspark.sql.functions import to_date, udf, expr
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType


def date_converting(date1):
    return "hello"


conf = SparkConf()
conf.set("spark.app.name", "UDF Demo Spark application")
spark = SparkSession.builder.config(conf=conf).getOrCreate()
conf = spark.sparkContext.getConf()
var1 = conf.get("spark.app.name")
print("Application name is" + var1)

my_schema = StructType(
    [
        StructField("ID", StringType()),
        StructField("Name", StringType()),
        StructField("DOB", StringType()),
        StructField("Salary", StringType())

    ]
)

my_rows = [
    Row("100", "palani", "19810816", "10000"),
    Row("200", "Seetha", "19860503", "20000"),
    Row("300", "varnikhaa sree", "20081207", "30000"),
    Row("400", "Adwaidan", "20160602", "40000")
]
# Creating an RDD over here
my_rdd = spark.sparkContext.parallelize(my_rows)
# Creating a DF from RDD and setting the schema for the DF
my_df = spark.createDataFrame(my_rdd, my_schema)
my_df.printSchema()
my_df.show()
# By using UDF, registering the function as dataframe python function,
# here date_converting is a function and this function returning
# StringType
register1 = udf(date_converting, StringType())
new_my_df = my_df.withColumn("DOB", register1("DOB"))
new_my_df.printSchema()
new_my_df.show()
# By using UDF Here we are registering function to the catalog
spark.udf.register("register2", date_converting, StringType())
# Listing the available function after registration
[print(f) for f in spark.catalog.listFunctions() if "register2" in f.name]
new1_my_df = my_df.withColumn("DOB", expr("register2(DOB)"))
new1_my_df.show()
