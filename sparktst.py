from pyspark import SparkConf, SparkContext
sc = SparkContext(master="local",appName="Spark Demo")
print(sc.textFile("/Users/palaniappanparamasivam/Documents/Palani/testing_project/spark_project/testfile").first())