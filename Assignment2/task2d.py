import sys
from csv import reader
from pyspark import SparkContext

sc = SparkContext()
# read raw file
all_trip = sc.textFile(sys.argv[1], 1).mapPartitions(lambda x: reader(x))

# each taxi's total trips
total_trips = all_trip.map(lambda x: (x[0], 1))
total_trips = total_trips.reduceByKey(lambda x, y: x + y)

# each taxi's driven day
days_driven = all_trip.map(lambda x: ((x[0], x[3][0:10]), 1))
days_driven = days_driven.reduceByKey(lambda x, y: x + y)
days_driven = days_driven.map(lambda x: (x[0][0], 1))
days_driven = days_driven.reduceByKey(lambda x, y: x + y)

# join and sort by medallion
result = total_trips.join(days_driven).map(lambda x: (x[0], x[1][0], x[1][1], float((x[1][0] / x[1][1]))))
result = result.sortBy(lambda x: x[0])

# format to output
output = result.map(lambda x: x[0] + ',' + str(x[1]) + ',' + str(x[2]) + ',' + "{0:.2f}".format(x[3]))
output.saveAsTextFile("task2d.out")
