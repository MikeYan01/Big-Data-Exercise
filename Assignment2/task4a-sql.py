import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task4a-sql").config("spark.some.config.option", "some-value").getOrCreate()

# read data
all_trip = spark.read.format('csv').options(header = 'false', inferschema = 'true').load(sys.argv[1])

# create view
all_trip.createOrReplaceTempView("trip")

result = spark.sql("SELECT _c16 AS vehicle_type, COUNT(*) AS total_trips, SUM(_c5) AS total_revenue, SUM(_c8 / _c5) / COUNT(*) * 100 AS avg_tip_percentage FROM trip GROUP BY _c16 ORDER BY _c16")

# format string
result.select(format_string('%s,%d,%.2f,%.2f', result.vehicle_type, result.total_trips, result.total_revenue, result.avg_tip_percentage)).write.save("task4a-sql.out", format = "text")