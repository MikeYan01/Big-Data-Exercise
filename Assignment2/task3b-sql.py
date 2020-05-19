import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task3b-sql").config("spark.some.config.option", "some-value").getOrCreate()

# read data
all_trip = spark.read.format('csv').options(header = 'false', inferschema = 'true').load(sys.argv[1])

# create view
all_trip.createOrReplaceTempView("trip")

result = spark.sql("SELECT _c0 AS medallion, timestamp(_c3) AS pickup_datetime FROM trip GROUP BY _c0, timestamp(_c3) HAVING COUNT(*) > 1 ORDER BY medallion, pickup_datetime")

# format string
result.select(format_string('%s,%s', result.medallion, date_format(result.pickup_datetime,'yyyy-MM-dd HH:mm:ss'))).write.save("task3b-sql.out", format = "text")