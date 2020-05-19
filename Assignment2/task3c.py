import sys
from csv import reader
from pyspark import SparkContext

sc = SparkContext()
# read raw file
all_trip = sc.textFile(sys.argv[1], 1).mapPartitions(lambda x: reader(x))

# each taxi's total trips
total_trips = all_trip.map(lambda x: (x[0], 1))
total_trips = total_trips.reduceByKey(lambda x, y: x + y)

# each taxi's trips without GPS
GPS_fail_trips = all_trip.filter(lambda x: float(x[10]) == 0 and float(x[11]) == 0 and float(x[12]) == 0 and float(x[13]) == 0)
GPS_fail_trips = GPS_fail_trips.map(lambda x: (x[0], 1))
GPS_fail_trips = GPS_fail_trips.reduceByKey(lambda x, y: x + y)

# left outer join
result = total_trips.leftOuterJoin(GPS_fail_trips)

# transform 'NoneType' into 0, then sort
result = result.map(lambda x: (x[0], str(x[1][0]), '0' if x[1][1] is None else str(x[1][1])))
result = result.sortBy(lambda x: x[0])

# format to output
output = result.map(lambda x: x[0] + ',' + "{0:.2f}".format( float(x[2]) / float(x[1]) * 100 ))
output.saveAsTextFile("task3c.out")
