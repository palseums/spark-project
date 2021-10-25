import sys

from pyspark import SparkConf
from pyspark.sql import *
from lib.utils import get_spark_app_config, load_survey_df
conf = SparkConf()
conf = get_spark_app_config()
spark = SparkSession.builder.config(conf=conf).getOrCreate()
if len(sys.argv) != 2:
    sys.exit(-1)
survey_df = load_survey_df(spark, sys.argv[1])
partitioned_survey_df = survey_df.repartition(2)
# survey_df.where("age < 40").select("name", "salary", "country").groupBy("country").count()

where_df = partitioned_survey_df.filter("salary < 12000")
filter_df = where_df.select("name", "salary")
filter_df.show()
# input ("Press Enter")

