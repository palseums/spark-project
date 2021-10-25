import configparser
from pyspark import SparkConf
def get_spark_app_config():
    conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")
    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        conf.set(key,val)
    return conf

def load_survey_df(spark, data_file):
    # You are telling header is there in this file
    # You assume the data type by yourself by specifying inferSchema
    return spark.read.option("header", "true").option("inferSchema", "true").csv(data_file)