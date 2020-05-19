#!/usr/bin/env python

import sys

# store all licenses and trips values of one key
all_licenses, all_trips = [], []

# key of last line
last_line_key = ''

# general output function
def output():
    for trip in all_trips:
        for license in all_licenses:
            print(last_line_key + '\t' + trip + ',' + license)

for line in sys.stdin:
    current_key, value = line.strip().split('\t') 

    # key has updated, output and reset value
    if current_key != last_line_key and last_line_key:
        output()
        all_licenses, all_trips = [], []

    # determine value's source
    if value[0:2] == 'lc':
        all_licenses.append(value)
    elif value[0:2] == 'tf':
        all_trips.append(value)

    last_line_key = current_key

# output last key & value
output()
