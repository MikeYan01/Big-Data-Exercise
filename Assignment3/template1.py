import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark import SparkContext

# Read in and setup. DO NOT CHANGE.
sc = SparkContext()
spark = SparkSession.builder.appName("hw3").config("spark.some.config.option", "some-value").getOrCreate()

# Load Data
parking_df = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[1])
parking_df.createOrReplaceTempView("parking")

##################
##### Task1 ######  
##################
# Task 1-a

# filter duplicate and null key
task1a_result = parking_df.count() - spark.sql("SELECT summons_number, COUNT(*) AS count FROM parking WHERE summons_number IS NOT NULL GROUP BY summons_number").count()

##################
# Task 1-b

task1b_result = spark.sql("SELECT plate_type, COUNT(*) AS count_ FROM parking GROUP BY plate_type ORDER BY count_ DESC")

##################
# Task 1-c

updated_parking = parking_df.withColumn('plate_type', F.when(parking_df['plate_type'] == '999', 'NULL').otherwise(parking_df['plate_type']))
updated_parking.createOrReplaceTempView("updated_parking")
task1c_result = spark.sql("SELECT plate_type, COUNT(*) AS count FROM updated_parking GROUP BY plate_type ORDER BY count DESC") 

##################
#Task 1-d

task1d_result1 = spark.sql("SELECT COUNT(*) AS count FROM parking WHERE violation_county IS null").first()[0]

removed_parking = parking_df.filter(parking_df['violation_county'] != 'null')
removed_parking.createOrReplaceTempView("removed_parking")
task1d_result2 = spark.sql("SELECT COUNT(*) AS count FROM removed_parking").first()[0]

##################

"""
Output Method - Do Not Change. UNCOMMENT the following lines when you have the tasks finished
"""
sc.parallelize([str(task1a_result)]).saveAsTextFile("hw3-task1-a.out")

task1b_result.coalesce(1).rdd.map(lambda x: x[0] + ',' + str(x[1])).saveAsTextFile("hw3-task1-b.out")

task1c_result.coalesce(1).rdd.map(lambda x: x[0] + ',' + str(x[1])).saveAsTextFile("hw3-task1-c.out")

sc.parallelize([str(task1d_result1)]).saveAsTextFile("hw3-task1-d1.out")
sc.parallelize([str(task1d_result2)]).saveAsTextFile("hw3-task1-d2.out")

###################
##### Task2 #######
###################

# Convert relevant columns to RDD here
temp1 = spark.sql("SELECT (CASE WHEN plate_id IS NULL THEN 'null' ELSE plate_id END) AS plate_id FROM parking")
temp2 = spark.sql("SELECT (CASE WHEN street_name IS NULL THEN 'null' ELSE street_name END) AS street_name FROM parking")

plate_id_rdd = temp1.rdd.map(lambda x: x[0])
street_name_rdd = temp2.rdd.map(lambda x: x[0])

##################
# Task 2-a: Implementing Fingerprint

import string, unicodedata
import re

def preprocess(value):
    # remove leading and trailing white space
    value = value.strip()

    # change all characters to their lowercase representation
    value = value.lower()

    # remove all punctuation and control characters
    PUNCTUATION = re.compile('[%s]' % re.escape(string.punctuation))
    value = PUNCTUATION.sub('', value)

    return value

# normalize extended western characters to their ASCII representation (for example "gödel" → "godel")
def latinize(value):
    return unicodedata.normalize('NFKD', value).encode('ascii', 'ignore')

# sort the tokens and remove duplicates
def unique_preserving_order(seq):
    seen = set()
    return [x for x in seq if not (x in seen or seen.add(x))]

def fingerprint(value):
    key = preprocess(value)

    # join the tokens back together
    key = latinize(' '.join(
        # split the string into whitespace-separated tokens
        unique_preserving_order(sorted(key.split()))
    )).decode("utf-8")

    return (key, value)
    
#################
#Task 2-b: Implementing N-gram Fingerprint

def ngram_fingerprint(value, n = 1): #choose your n-gram
    key = preprocess(value)

    # specifically, remove all whitespace
    key = key.replace(' ', '')

    # join the sorted n-grams back together
    key = latinize(''.join(
        # obtain all the string n-grams, sort the n-grams and remove duplicates
        unique_preserving_order(sorted([key[i : i + n] for i in range(len(key) - n + 1)]))
    )).decode("utf-8")
    
    return (key, value)

##################
#Task 2-c: Apply Fingerprint to the plate_id column. Output all output clusters.

task2c_table = []
for each in plate_id_rdd.collect():
    task2c_table.append(fingerprint(each))

# group values by key, keep each pair's order, filter out the cluster
task2c_result = sc.parallelize(task2c_table).groupByKey().mapValues(list).map(lambda x: [x[0], sorted(set(x[1]), key = x[1].index)]).filter(lambda x: len(x[1]) > 1)

###################
#Task 2-d: Apply N-Gram Fingerprint to the plate_id column. Output all output clusters.

task2d_table = []
for each in plate_id_rdd.collect():
    task2d_table.append(ngram_fingerprint(each, 3))

# group values by key, keep each pair's order, filter out the cluster
task2d_result = sc.parallelize(task2d_table).groupByKey().mapValues(list).map(lambda x: [x[0], sorted(set(x[1]), key = x[1].index)]).filter(lambda x: len(x[1]) > 1)

###################
#Task 2-e: Apply Fingerprint to the street_name column. Output the first 20 clusters.

task2e_table = []
for each in street_name_rdd.collect():
    task2e_table.append(fingerprint(each))

# group values by key, keep each pair's order, filter out the cluster, take top 20
task2e_result = sc.parallelize(task2e_table).groupByKey().mapValues(list).map(lambda x: [x[0], sorted(set(x[1]), key = x[1].index)]).filter(lambda x: len(x[1]) > 1).take(20)
task2e_result = sc.parallelize(task2e_result)

##################
#Task 2-f: Apply N-Gram Fingerprint to the street_name column. Output the first 20 clusters

task2f_table = []
for each in street_name_rdd.collect():
    task2f_table.append(ngram_fingerprint(each))

# group values by key, keep each pair's order, filter out the cluster, take top 20
task2f_result = sc.parallelize(task2f_table).groupByKey().mapValues(list).map(lambda x: [x[0], sorted(set(x[1]), key = x[1].index)]).filter(lambda x: len(x[1]) > 1).take(20)
task2f_result = sc.parallelize(task2f_result)

##################
# Task 2-g: Provide your qualitative response in template2.txt.
##################
# Task 2-h: Design and perform transformations to the street_name column here. Output all clusters. 

# 1. everything uppercase
task2h_rdd = street_name_rdd.map(lambda x: x.upper())

# 2. filter out punctuation
PUNCTUATION = re.compile('[%s]' % re.escape(string.punctuation))
task2h_rdd = task2h_rdd.map(lambda x: PUNCTUATION.sub('', x))

# 3. trim all leading and trailing whitespace
task2h_rdd = task2h_rdd.map(lambda x: x.strip())

# 4. collapse consecutive whitespace
task2h_rdd = task2h_rdd.map(lambda x: ' '.join(x.split()))

task2h_table = []
for each in task2h_rdd.collect():
    task2h_table.append(fingerprint(each))

# group values by key, keep each pair's order, filter out the cluster
task2h_result = sc.parallelize(task2h_table).groupByKey().mapValues(list).map(lambda x: [x[0], sorted(set(x[1]), key = x[1].index)]).filter(lambda x: len(set(x[1])) > 1)

#################
# Task 2-i: Provide your qualitative response in template2.txt.
#################

"""
Output Methods - Do not Change. UNCOMMENT these lines when you have the tasks finished
"""

task2c_result.map(lambda x: x[0] + ',' + ','.join(x[1])).saveAsTextFile("hw3-task2-c.out")
task2d_result.map(lambda x: x[0] + ',' + ','.join(x[1])).saveAsTextFile("hw3-task2-d.out")
task2e_result.map(lambda x: x[0] + ',' + ','.join(x[1])).saveAsTextFile("hw3-task2-e.out")
task2f_result.map(lambda x: x[0] + ',' + ','.join(x[1])).saveAsTextFile("hw3-task2-f.out")
task2h_result.map(lambda x: x[0] + ',' + ','.join(x[1])).saveAsTextFile("hw3-task2-h.out")