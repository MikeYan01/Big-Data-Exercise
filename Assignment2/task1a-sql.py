import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task1a-sql").config("spark.some.config.option", "some-value").getOrCreate()

# read data
trip = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[1])
fare = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[2])

# create view
trip.createOrReplaceTempView("trip")
fare.createOrReplaceTempView("fare")

# inner join
# store_and_fwd_flag has some missing value, remain ''
result = spark.sql("SELECT medallion, hack_license, vendor_id, pickup_datetime, rate_code, (CASE when (store_and_fwd_flag = '' OR store_and_fwd_flag IS NULL) THEN '' ELSE store_and_fwd_flag END) AS store_and_fwd_flag, dropoff_datetime, passenger_count, trip_time_in_secs, trip_distance, pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude, payment_type, fare_amount, surcharge, mta_tax, tip_amount, tolls_amount, total_amount FROM trip t NATURAL JOIN fare f ORDER BY medallion, hack_license, pickup_datetime") 

# format string
result.select(format_string('%s,%s,%s,%s,%s,%s,%s,%d,%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s', result.medallion, result.hack_license, result.vendor_id, date_format(result.pickup_datetime,'yyyy-MM-dd HH:mm:ss'), result.rate_code, result.store_and_fwd_flag, date_format(result.dropoff_datetime,'yyyy-MM-dd HH:mm:ss'), result.passenger_count, result.trip_time_in_secs, result.trip_distance, result.pickup_longitude, result.pickup_latitude, result.dropoff_longitude, result.dropoff_latitude, result.payment_type, result.fare_amount, result.surcharge, result.mta_tax, result.tip_amount, result.tolls_amount, result.total_amount)).write.save("task1a-sql.out", format = "text")