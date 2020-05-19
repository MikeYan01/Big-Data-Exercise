#!/usr/bin/env python

import sys

# key, medallion of last line
last_line_key, last_line_medallion = '', ''

# count for each driver's taxis
count = 1

for line in sys.stdin:
    current_key, current_medallion = line.strip().split(',')

    # key has updated, output and reset value
    if current_key != last_line_key and last_line_key != '':
        print (last_line_key + '\t' + str(count))
        count = 1

    # only update count when medallion changes
    elif current_medallion != last_line_medallion and last_line_key != '':
        count += 1

    last_line_key, last_line_medallion = current_key, current_medallion

# output the last key and value
print (last_line_key + '\t' + str(count))