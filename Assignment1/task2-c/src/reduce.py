#!/usr/bin/env python

import sys

# key of last line
last_line_key = ''

# each passenger_count
count = 0

for line in sys.stdin:
    current_key, value = line.strip().split('\t')
    
    # key has updated, output and reset value
    if current_key != last_line_key and last_line_key != '':
        print (last_line_key + '\t' + str(count))
        count = 0
    
    # always add 1 to current passenger_count
    count += 1

    last_line_key = current_key

# output last key & value
print (last_line_key + '\t' + str(count))