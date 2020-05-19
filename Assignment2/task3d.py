import sys
from csv import reader
from pyspark import SparkContext

sc = SparkContext()
# read raw file
all_trip = sc.textFile(sys.argv[1], 1).mapPartitions(lambda x: reader(x))

# each driver's taxi usage
total_trips = all_trip.map(lambda x: ((x[1], x[0]), 1))
total_trips = total_trips.reduceByKey(lambda x, y: x + y)
total_trips = total_trips.map(lambda x: (x[0][0], 1))
total_trips = total_trips.reduceByKey(lambda x, y: x + y)

# sort by hack_license
result = total_trips.sortBy(lambda x: x[0])

# format to output
output = result.map(lambda x: x[0] + ',' + str(x[1]))
output.saveAsTextFile("task3d.out")
