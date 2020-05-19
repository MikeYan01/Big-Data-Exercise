#!/usr/bin/env python

import sys

# key of last line
last_line_key = -1

# count each range's occurence
count = 0

# general output function
def output(last_line_key, num):
    if last_line_key == 0:
        print(str(last_line_key) + ',' + str(last_line_key + 4) + '\t' + str(num))
    elif last_line_key == 48:
        print(str(round(last_line_key + 0.01, 2)) + ',infinite\t' + str(num))
    else:
        print(str(round(last_line_key + 0.01, 2)) + ',' + str(last_line_key + 4) + '\t' + str(num))

for line in sys.stdin:
    # get each range's start and value
    line = line.strip().split('\t')
    current_key, value = int(line[0][1:]), int(line[1])

    # key is updated, output current range's result
    if current_key != last_line_key and last_line_key != -1:
        output(last_line_key, count)
        count = 0
    
    # always add 1 to current range
    count += 1

    last_line_key = current_key

# output last range's result
output(last_line_key, count)
