import sys
from csv import reader
from pyspark import SparkContext

sc = SparkContext()
# read raw file
all_trip = sc.textFile(sys.argv[1], 1).mapPartitions(lambda x: reader(x))

result = all_trip.map(lambda x: (x[7], 1)).reduceByKey(lambda x, y: x + y)

# format to output
output = result.map(lambda x: x[0] + ',' + str(x[1]))

# sort by num_of_passengers
output = output.sortBy(lambda x: x[0])

output.saveAsTextFile("task2b.out")
