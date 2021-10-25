from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[3]").appName("miscdemo").getOrCreate()
df = spark.read\
    .format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("data/data.csv")
df.show()
df.printSchema()
changed_date_df = df.withColumn("Date", to_date("Date", "MM/dd/yyyy"))
changed_date_df.printSchema()
changed_date_df.show()
Agg_df = changed_date_df.groupBy("country").agg(count(col("country").alias("palani")))
# The above one can be implemented like below also
# we can create a variable and do the same
var1 = count(col("country").alias("palani"))
var2 = sum(col("group")).alias("sum of group")
Agg1_df = changed_date_df.groupBy("country").agg(var1, var2)
Agg_df.show()
Agg1_df.show()
#Agg1_df.coalesce(1).write\
 #   .format("parquet")\
  #  .mode("overwrite")\
   # .save("output")
#Agg1_df.sort("country")

# We can do above all with select statement also
new_df = changed_date_df.select(count("*").alias("count *"), sum("group").alias("group"))
new_df.show()
changed_date_df.selectExpr("count(1) as `count`", "count(country) as `count field`", "sum(group) as `sum of group`", "avg(group) as `avg of group`").show()
# You can use a dataframe to create a temp view
changed_date_df.createOrReplaceTempView("sales")
# sql expression will return a data frame
summary_sql = spark.sql("""
select * from sales
""")
summary_sql.show()
# Below are some examples of how to use expression
summary-df = invoice_df \
    .groupBy("country","InvoiceNo")\
    .agg(sum("quantity").alias("Totalquantity"),\
         round(sum(expr("quantity * unitprice")),2)).alias("Invoicevalue"),\
        expr("round(sum(expr("quantity * unitprice")),2) as Invoicevalue")