from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
sc = SparkContext()
spark = SparkSession(sc)
lines = spark.sparkContext.textFile("/Users/palaniappanparamasivam/PycharmProjects/pythonProject/SparkProject/data/test.txt")
print("{0}{1}".format("The number of partition is ",lines.getNumPartitions()))
print("{0}{1}".format("The type of lines variable is ",type(lines)))
l1=['s1','s2','s3','s4','s5']
#schema_lst = ["State","Cases","Recovered","Deaths"]
#rdd1 = spark.sparkContext.parallelize(input_data)
#rdd2 = lines.toDF(l1).write.csv("csv1")
#lines1 = lines.toDF("string1")

#print(lines.collect())
# map function
map1 = lines.map(lambda x: x.split(","))
# flatmap function
flatmap1 = lines.flatMap(lambda x: x.split(","))

print("{0}{1}".format("The type of flatmap1 variable is ",type(flatmap1)))

print("Output of the map1")
for element in map1.collect():
    print(element)
print("Output of the flatmap1")
for element in flatmap1.collect():
    print(element)
count_map = flatmap1.map(lambda x: (x,1))
count_flatmap = flatmap1.flatMap(lambda x:(x,1))

print("{0}{1}".format("The type of count1 variable is ",type(count_map)))

print("The output of count_map ")
for element in count_map.collect():
    print(element)

print("The output of count_flatmap ")
for element in count_flatmap.collect():
    print(element)

value = count_map.reduceByKey(lambda a,b: a+b)

print("{0}{1}".format("The type of value variable is ",type(value)))

for element in value.collect():
    print(element)

