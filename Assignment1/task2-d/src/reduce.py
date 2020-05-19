#!/usr/bin/env python

import sys

# key of last line
last_line_key = ''

## total revenue and total toll
total_revenue, total_toll = 0, 0

for line in sys.stdin:
    current_key, value = line.strip().split('\t')
    
    # get current revenue and toll
    revenue = float(value.strip().split(',')[0])
    toll = float(value.strip().split(',')[1])

    # key has updated, output and reset value
    if current_key != last_line_key and last_line_key != '':
        print('%s\t%.2f,%.2f' % (last_line_key, total_revenue, total_toll))
        total_revenue, total_toll = 0, 0

    # always add current revenue and toll to total amounts
    total_revenue += revenue
    total_toll += toll

    last_line_key = current_key

# output last key and value
print('%s\t%.2f,%.2f' % (last_line_key, total_revenue, total_toll))