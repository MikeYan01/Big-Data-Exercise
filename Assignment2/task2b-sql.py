import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task2b-sql").config("spark.some.config.option", "some-value").getOrCreate()

# read data
all_trip = spark.read.format('csv').options(header = 'false', inferschema = 'true').load(sys.argv[1])

# create view
all_trip.createOrReplaceTempView("trip")

# Distribution of the number of passengers
result = spark.sql("SELECT _c7 AS num_of_passengers, count(_c7) AS num_trips FROM trip GROUP BY _c7 ORDER BY _c7")

result.select(format_string('%d,%d', result.num_of_passengers, result.num_trips)).write.save("task2b-sql.out", format = "text")