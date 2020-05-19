import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task3d-sql").config("spark.some.config.option", "some-value").getOrCreate()

# read data
all_trip = spark.read.format('csv').options(header = 'false', inferschema = 'true').load(sys.argv[1])

# create view
all_trip.createOrReplaceTempView("trip")

result = spark.sql("SELECT _c1 AS hack_license, COUNT(DISTINCT _c0) AS num_taxis_used FROM trip GROUP BY _c1 ORDER BY _c1")

# format string
result.select(format_string('%s,%d', result.hack_license, result.num_taxis_used)).write.save("task3d-sql.out", format = "text")