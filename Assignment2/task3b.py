import sys
from csv import reader
from pyspark import SparkContext

sc = SparkContext()
# read raw file
all_trip = sc.textFile(sys.argv[1], 1).mapPartitions(lambda x: reader(x))

# (medallion, pickup_datetime) as a key
result = all_trip.map(lambda x: ((x[0], x[3]), 1)).reduceByKey(lambda x, y: x + y)
result = result.filter(lambda x: x[1] > 1)

# sort by medallion, pickup_datetime
result = result.sortBy(lambda x: (x[0][0], x[0][1]))

# format to output
output = result.map(lambda x: x[0][0] + ',' + x[0][1])
output.saveAsTextFile("task3b.out")
