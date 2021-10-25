from pyspark import SparkConf
from pyspark.sql import *
from lib.logger import Log4j
from lib.utils import get_spark_app_config

# SPARK_HOME Value is /Users/palaniappanparamasivam/Documents/install/spark-3.1.2-bin-hadoop2.7
if __name__ == "__main__":
    # Another method of setting the configuration
    # First method of creating a session and setting configuration variable
    # conf = SparkConf()
    # conf.set("spark.app.name", "Hello Spark")
    # conf.set("spark.master", "local[3]")
    # spark = SparkSession.builder.config(conf=conf).getOrCreate()
    # Second method of creating a session and setting configuration variable
    #spark = SparkSession.builder.appName("Hello Spark").master("local[3]").getOrCreate()
    conf = get_spark_app_config()
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    conf = spark.sparkContext.getConf()  # To get the configuration from the spark session
    app_name = conf.get("spark.app.name")
    print("Application name is " + app_name)
    print("How are you")
    logger = Log4j(spark)
    logger.info("Starting Hellospark")
    logger.info("Ending Hellospark")
    logger.info(conf.toDebugString())  # This just prints all the configuration from the spark session
    spark.stop()
