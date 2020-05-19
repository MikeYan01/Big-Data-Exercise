import sys
from csv import reader
from pyspark import SparkContext

sc = SparkContext()
# read raw file
fare_samp = sc.textFile(sys.argv[1], 1).mapPartitions(lambda x: reader(x))
license_samp = sc.textFile(sys.argv[2], 1).mapPartitions(lambda x: reader(x))

# skip first line
fare_samp = fare_samp.filter(lambda x: x[0][0] != 'm')
license_samp = license_samp.filter(lambda x: x[0][0] != 'm')

# join according to key and value
fare = fare_samp.map(lambda x: ((x[0]), (x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10])))
license = license_samp.map(lambda x: ((x[0]), (x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10], x[11], x[12], x[13], x[14], x[15])))
result = fare.join(license)

# format and sort output, need to specifically handle 'name' column
output = result.map(lambda x: (x[0], x[1][0][0], x[1][0][1], x[1][0][2], x[1][0][3], x[1][0][4], x[1][0][5], x[1][0][6], x[1][0][7], x[1][0][8], x[1][0][9], '"' + x[1][1][0] + '"', x[1][1][1], x[1][1][2], x[1][1][3], x[1][1][4], x[1][1][5], x[1][1][6], x[1][1][7], x[1][1][8], x[1][1][9], x[1][1][10], x[1][1][11], x[1][1][12], x[1][1][13], x[1][1][14]))
output = output.sortBy(lambda x: (x[0], x[1], x[3]))
output = output.map(lambda x: ','.join(x[:]))

output.saveAsTextFile('task1b.out')