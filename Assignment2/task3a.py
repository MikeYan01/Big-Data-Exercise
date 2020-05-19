import sys
from csv import reader
from pyspark import SparkContext

sc = SparkContext()
# read raw file
all_trip = sc.textFile(sys.argv[1], 1).mapPartitions(lambda x: reader(x))

# total invalid fare amounts
invalid_count = all_trip.filter(lambda x: float(x[15]) < 0).count()

# format to output
output = sc.parallelize([str(invalid_count)])
output.saveAsTextFile("task3a.out")
