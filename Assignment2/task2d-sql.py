import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task2d-sql").config("spark.some.config.option", "some-value").getOrCreate()

# read data
all_trip = spark.read.format('csv').options(header = 'false', inferschema = 'true').load(sys.argv[1])

# create view
all_trip.createOrReplaceTempView("trip")

result = spark.sql("SELECT _c0 AS medallion, COUNT(*) AS total_trips, COUNT(DISTINCT date(_c3)) AS days_driven, COUNT(*) / COUNT(DISTINCT date(_c3)) AS average FROM trip GROUP BY _c0 ORDER BY _c0")

# format string
result.select(format_string('%s,%d,%d,%.2f', result.medallion, result.total_trips, result.days_driven, result.average)).write.save("task2d-sql.out", format = "text")