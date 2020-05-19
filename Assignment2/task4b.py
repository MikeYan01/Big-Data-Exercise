import sys
from csv import reader
from pyspark import SparkContext

sc = SparkContext()
# read raw file
all_trip = sc.textFile(sys.argv[1], 1).mapPartitions(lambda x: reader(x))

# total trips
total_trips = all_trip.map(lambda x: (x[18], 1))
total_trips = total_trips.reduceByKey(lambda x, y: x + y)

# total revenue
total_revenue = all_trip.map(lambda x: (x[18], float(x[5])))
total_revenue = total_revenue.reduceByKey(lambda x, y: x + y)

# total tip percentage
total_tip_percentage = all_trip.map(lambda x: (x[18], 0 if float(x[5]) == 0 else float(x[8]) / float(x[5])))
total_tip_percentage = total_tip_percentage.reduceByKey(lambda x, y: x + y)

# join trips, revenue, total tip percentage
temp = total_trips.join(total_revenue).join(total_tip_percentage)

# map required attributes and sort
result = temp.map(lambda x: (x[0], x[1][0][0], x[1][0][1], x[1][1] / x[1][0][0] * 100))
result = result.sortBy(lambda x: x[0])

# format to output
output = result.map(lambda x: x[0] + ',' + str(x[1]) + ',' + str("{0:.2f}".format(x[2])) + ',' + str("{0:.2f}".format(x[3])))
output.saveAsTextFile("task4b.out")
