from pyspark import Sparkconf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession



myconfig = Sparkconf().set("spark.rpc.message.maxSize","512")
sc = SparkContext.getOrCreate(conf=myconfig)
# or we can do it in the below way
sc = SparkSession.builder.appName("abc").getOrCreate(conf=myconfig)