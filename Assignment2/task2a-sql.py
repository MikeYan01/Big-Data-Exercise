import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task2a-sql").config("spark.some.config.option", "some-value").getOrCreate()

# read data
all_trip = spark.read.format('csv').options(header = 'false', inferschema = 'true').load(sys.argv[1])

# create view
all_trip.createOrReplaceTempView("trip")

# select from table, and get each range's count
trip0_5 = spark.sql("SELECT count(*) FROM trip WHERE _c15 >= 0 AND _c15 <= 5").first()[0]
trip5_15 = spark.sql("SELECT count(*) FROM trip WHERE _c15 > 5 AND _c15 <= 15").first()[0]
trip15_30  = spark.sql("SELECT count(*) FROM trip WHERE _c15 > 15 AND _c15 <= 30").first()[0]
trip30_50 = spark.sql("SELECT count(*) FROM trip WHERE _c15 > 30 AND _c15 <= 50").first()[0]
trip50_100 = spark.sql("SELECT count(*) FROM trip WHERE _c15 > 50 AND _c15 <= 100").first()[0]
trip100 = spark.sql("SELECT count(*) FROM trip WHERE _c15 > 100").first()[0]

# create result dataframe
result = [{"amount_range": "0,5", "num_trips": str(trip0_5)}, {"amount_range": "5,15", "num_trips": str(trip5_15)}, {"amount_range": "15,30", "num_trips": str(trip15_30)}, {"amount_range": "30,50", "num_trips": str(trip30_50)}, {"amount_range": "50,100", "num_trips": str(trip50_100)}, {"amount_range": ">100", "num_trips": str(trip100)}]
result = spark.createDataFrame(result)

# format string
result.select(format_string('%s,%s', result.amount_range, result.num_trips)).write.save("task2a-sql.out", format = "text")