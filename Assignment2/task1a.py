import sys
from csv import reader
from pyspark import SparkContext

sc = SparkContext()
# read raw file
trip_samp = sc.textFile(sys.argv[1], 1).mapPartitions(lambda x: reader(x))
fare_samp = sc.textFile(sys.argv[2], 1).mapPartitions(lambda x: reader(x))

# skip first line
trip_samp = trip_samp.filter(lambda x: x[0][0] != 'm')
fare_samp = fare_samp.filter(lambda x: x[0][0] != 'm')

# join according to key and value
trip = trip_samp.map(lambda x: ((x[0], x[1], x[2], x[5]), (x[3], x[4], x[6], x[7], x[8], x[9], x[10], x[11], x[12], x[13])))
fare = fare_samp.map(lambda x: ((x[0], x[1], x[2], x[3]), (x[4], x[5], x[6], x[7], x[8], x[9], x[10])))
result = trip.join(fare)

# format and sort output
output = result.map(lambda x: (x[0][0], x[0][1], x[0][2], x[0][3], x[1][0][0], x[1][0][1], x[1][0][2], x[1][0][3], x[1][0][4], x[1][0][5], x[1][0][6], x[1][0][7], x[1][0][8], x[1][0][9], x[1][1][0], x[1][1][1], x[1][1][2], x[1][1][3], x[1][1][4], x[1][1][5], x[1][1][6]))
output = output.sortBy(lambda x: (x[0], x[1], x[3]))
output = output.map(lambda x: ','.join(x[:]))

output.saveAsTextFile('task1a.out')