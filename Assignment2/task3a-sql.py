import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task3a-sql").config("spark.some.config.option", "some-value").getOrCreate()

# read data
all_trip = spark.read.format('csv').options(header = 'false', inferschema = 'true').load(sys.argv[1])

# create view
all_trip.createOrReplaceTempView("trip")

result = spark.sql("SELECT COUNT(*) AS invalid_fare FROM trip WHERE _c15 < 0")

# format string
result.select(format_string('%d', result.invalid_fare)).write.save("task3a-sql.out", format = "text")