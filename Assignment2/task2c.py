import sys
from csv import reader
from pyspark import SparkContext

sc = SparkContext()
# read raw file
all_trip = sc.textFile(sys.argv[1], 1).mapPartitions(lambda x: reader(x))

# total revenue
revenue = all_trip.map(lambda x: (x[3][0:10], float(x[15]) + float(x[16]) + float(x[18])))
revenue = revenue.reduceByKey(lambda x, y: x + y)

# total toll
toll = all_trip.map(lambda x: (x[3][0:10], float(x[19])))
toll = toll.reduceByKey(lambda x, y: x + y)

# join and sort by date
result = revenue.join(toll)
result = result.sortBy(lambda x: x[0])

# format to output
output = result.map(lambda x: x[0] + ',' + "{0:.2f}".format(x[1][0]) + ',' + "{0:.2f}".format(x[1][1]))
output.saveAsTextFile("task2c.out")
