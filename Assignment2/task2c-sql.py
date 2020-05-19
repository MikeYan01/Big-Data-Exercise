import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task2c-sql").config("spark.some.config.option", "some-value").getOrCreate()

# read data
all_trip = spark.read.format('csv').options(header = 'false', inferschema = 'true').load(sys.argv[1])

# create view
all_trip.createOrReplaceTempView("trip")

result = spark.sql("SELECT date(_c3) AS date, SUM(_c15) + SUM(_c16) + SUM(_c18) AS total_revenue, SUM(_c19) AS total_tolls FROM trip GROUP BY date(_c3) ORDER BY date(_c3)")

# format string
result.select(format_string('%s,%.2f,%.2f', date_format(result.date,'yyyy-MM-dd'), result.total_revenue, result.total_tolls)).write.save("task2c-sql.out", format = "text")