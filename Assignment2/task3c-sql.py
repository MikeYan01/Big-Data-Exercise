import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task3c-sql").config("spark.some.config.option", "some-value").getOrCreate()

# read data
all_trip = spark.read.format('csv').options(header = 'false', inferschema = 'true').load(sys.argv[1])

# create view
all_trip.createOrReplaceTempView("trip")

# count and join
GPS_fail = spark.sql("SELECT _c0 AS GPSmedallion, COUNT(*) AS GPS_fail FROM trip WHERE _c10 = 0 AND _c11 = 0 AND _c12 = 0 AND _c13 = 0 GROUP BY _c0")
total_trips = spark.sql("SELECT _c0 AS medallion, COUNT(*) AS total_trips FROM trip GROUP BY _c0")
result = total_trips.join(GPS_fail, total_trips.medallion == GPS_fail.GPSmedallion, "left_outer")

# calculate percentage
result.createOrReplaceTempView("temp")
result = spark.sql("SELECT medallion, IFNULL(GPS_fail, 0) / total_trips * 100 AS percentage_of_trips FROM temp ORDER BY medallion")

# format string
result.select(format_string('%s,%.2f', result.medallion, result.percentage_of_trips)).write.save("task3c-sql.out", format = "text")