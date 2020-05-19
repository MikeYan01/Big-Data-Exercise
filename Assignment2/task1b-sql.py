import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task1b-sql").config("spark.some.config.option", "some-value").getOrCreate()

# read data
fare = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[1])
license = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[2])

# create view
fare.createOrReplaceTempView("fare")
license.createOrReplaceTempView("license")

# inner join
# agent_website has some missing value, remain ''
result = spark.sql("SELECT medallion, hack_license, vendor_id, pickup_datetime, payment_type, fare_amount, surcharge, mta_tax, tip_amount, tolls_amount, total_amount, name, type,current_status, DMV_license_plate, vehicle_VIN_number, vehicle_type, model_year, medallion_type, agent_number, agent_name, agent_telephone_number, (CASE when (agent_website = '' OR agent_website IS NULL) THEN '' ELSE agent_website END) AS agent_website, agent_address, last_updated_date, last_updated_time FROM fare f NATURAL JOIN license l ORDER BY medallion, hack_license, pickup_datetime") 

# format string
result.select(format_string('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"%s",%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s', result.medallion, result.hack_license, result.vendor_id, date_format(result.pickup_datetime,'yyyy-MM-dd HH:mm:ss'), result.payment_type, result.fare_amount, result.surcharge, result.mta_tax, result.tip_amount, result.tolls_amount, result.total_amount, result.name, result.type, result.current_status, result.DMV_license_plate, result.vehicle_VIN_number, result.vehicle_type, result.model_year, result.medallion_type, result.agent_number, result.agent_name, result.agent_telephone_number, result.agent_website, result.agent_address, result.last_updated_date, result.last_updated_time)).write.save("task1b-sql.out", format = "text")