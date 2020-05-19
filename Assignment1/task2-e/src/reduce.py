#!/usr/bin/env python

import sys

# key, day of last line
last_line_key, last_line_day = '', ''

# total days and trips
total_days, total_trips = 1, 1

for line in sys.stdin:
    line = line.strip().split(',')
    current_key, current_day = line[0], line[1]

    # key has updated, output and reset value
    if current_key != last_line_key and last_line_key != '':
        average_trip = float(total_trips) / total_days
        print('%s\t%d,%.2f' % (last_line_key, total_trips, average_trip))
        total_days, total_trips = 1, 1

    # always update total trips; only update total days when day changes
    elif last_line_key != '':
        total_trips += 1
        if current_day != last_line_day:
            total_days += 1

    last_line_key, last_line_day = current_key, current_day

# output the last key, value
average_trip = float(total_trips) / total_days
print('%s\t%d,%.2f' % (last_line_key, total_trips, average_trip))
