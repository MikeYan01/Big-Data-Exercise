import sys
from csv import reader
from pyspark import SparkContext

sc = SparkContext()
# read raw file
all_trip = sc.textFile(sys.argv[1], 1).mapPartitions(lambda x: reader(x))

# classify by range and get count
trip0_5 = all_trip.filter(lambda x: float(x[15]) >= 0 and float(x[15]) <= 5).count()
trip5_15 = all_trip.filter(lambda x: float(x[15]) > 5 and float(x[15]) <= 15).count()
trip15_30 = all_trip.filter(lambda x: float(x[15]) > 15 and float(x[15]) <= 30).count()
trip30_50 = all_trip.filter(lambda x: float(x[15]) > 30 and float(x[15]) <= 50).count()
trip50_100 = all_trip.filter(lambda x: float(x[15]) > 50 and float(x[15]) <= 100).count()
trip100 = all_trip.filter(lambda x: float(x[15]) > 100).count()

# create existing dataset
result = []
result.append("0,5," + str(trip0_5))
result.append("5,15," + str(trip5_15))
result.append("15,30," + str(trip15_30))
result.append("30,50," + str(trip30_50))
result.append("50,100," + str(trip50_100))
result.append(">100," + str(trip100))

# format to RDD
output = sc.parallelize(result)

output.saveAsTextFile('task2a.out')