import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task4c-sql").config("spark.some.config.option", "some-value").getOrCreate()

# read data
all_trip = spark.read.format('csv').options(header = 'false', inferschema = 'true').load(sys.argv[1])

# create view
all_trip.createOrReplaceTempView("trip")

result = spark.sql("SELECT _c20 AS agent_name, SUM(_c5) AS total_revenue FROM trip GROUP BY _c20 ORDER BY total_revenue DESC, _c20 LIMIT 10")

# format string
result.select(format_string('%s,%.2f', result.agent_name, result.total_revenue)).write.save("task4c-sql.out", format = "text")