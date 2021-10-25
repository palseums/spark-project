from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[3]").appName("miscdemo").getOrCreate()
data_list = [("palani", "28", "1", "2002"), ("seetha", "23", "5", "81"), ("sree", "12", "12", "6"),
             ("Adwaidan", "23", "5", "81")]
raw_df = spark.createDataFrame(data_list)
raw_df.printSchema()
# Assign name to the column by using the function toDF
raw_df = spark.createDataFrame(data_list).toDF("name", "day", "month", "year")
raw_df.printSchema()
# Adding a new column called id
raw1_df = raw_df.withColumn("id", monotonically_increasing_id())
raw1_df.show()
# case statement example
raw2_df = raw1_df.withColumn("year", expr("""

case when year < 21 then year + 2000
when year < 100 then year + 1900
else year
end
"""))
raw2_df.show()
raw2_df.printSchema()
# how to use the cast function
raw3_df = raw1_df.withColumn("year", expr("""

case when year < 21 then cast(year as int ) + 2000
when year < 100 then cast (year as int ) + 1900
else year
end
"""))
raw3_df.show()
# how to use the cast function in different way but this will change the schema to Integer type
raw4_df = raw1_df.withColumn("year", expr("""

case when year < 21 then year + 2000
when year < 100 then year + 1900
else year
end
""").cast(IntegerType()))
raw4_df.show()
raw5_df = raw4_df.withColumn("month", (col("month") + 2).cast(IntegerType()))
# The above expression can be written as below
# expr should always be in quotation
# By using this cast we are changing the datatype of month to integer type
raw6_df = raw4_df.withColumn("month", expr("month + 2").cast(IntegerType()))
raw5_df.show()
raw6_df.show()
# Example to change the datatype of the column
change_df = raw_df.withColumn("day", col("day").cast(IntegerType()))\
    .withColumn("month", col("month").cast(IntegerType()))\
    .withColumn("year", col("year").cast(IntegerType()))
change_df.show()
change_df.printSchema()
# Another method for case function
df8 = change_df.withColumn("year1",\
                           when(col("year") < 21, col("year") + 2000)\
                           .when(col("year") < 100, col("year") + 1900)\
                           .otherwise(col("year")))
df8.show()
# Example for concat and adding a new column
# df8 = change_df.withColumn("dob", expr("to_date(concat(day,'/',month,'/',year),'d/M/y'"))\
#.drop("day","month","year")\
 #   .dropDuplicates(["name","dob"])\
  #  .sort(expr("dob desc"))

