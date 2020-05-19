#!/usr/bin/env python

import sys

# store all fares and trips values of one key
all_fares, all_trips = [], []

# key of last line
last_line_key = ''

# general output function
def output():
    for trip in all_trips:
        for fare in all_fares:
            print(last_line_key + '\t' + trip + ',' + fare)

for line in sys.stdin:
    current_key, value = line.strip().split('\t')

    # key has updated, output and reset value
    if current_key != last_line_key and last_line_key:
        output()
        all_fares, all_trips = [], []
    
    # add new value
    if value[0:4] == 'trip':
        all_trips.append(value[4:])
    elif value[0:4] == 'fare':
        all_fares.append(value[4:])

    last_line_key = current_key

# output last key & value
output()