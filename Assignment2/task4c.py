import sys
from csv import reader
from pyspark import SparkContext

sc = SparkContext()
# read raw file
all_trip = sc.textFile(sys.argv[1], 1).mapPartitions(lambda x: reader(x))

# total revenue
total_revenue = all_trip.map(lambda x: (x[20], float(x[5])))
total_revenue = total_revenue.reduceByKey(lambda x, y: x + y)

# sort and pick top 10
result = total_revenue.sortBy(lambda x: (-x[1], x[0])).take(10)
result = sc.parallelize(result)

# format to output and pick top 10
output = result.map(lambda x: x[0] + ',' + str( "{0:.2f}".format(x[1]) ))
output.saveAsTextFile("task4c.out")
