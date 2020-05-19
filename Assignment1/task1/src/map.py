#!/usr/bin/env python

import sys

for line in sys.stdin:
    # skip attribute name line
    if line.startswith('medallion'):
        continue
    else:
        # skip empty line
        if (len(line) <= 1):
            continue
    
        line = line.strip().split(',')
        medallion, hack_license, vendor_id = line[0], line[1], line[2]

        # determine file being read by column size
        # fares
        if len(line) == 11:
            pickup_datetime = line[3]
            
            # four key attributes
            part1 = medallion + ',' + hack_license + ',' + vendor_id + ',' + pickup_datetime
            
            # other attributes
            part2 = ','.join(line[4:])
            
            print(part1 + '\tfare' + part2)

        # trips
        elif len(line) == 14:
            pickup_datetime = line[5]
            
            # four key attributes
            part1 = medallion + ',' + hack_license + ',' + vendor_id + ',' + pickup_datetime

            # other attributes
            part2 = ','.join(line[3:5] + line[6:])
            
            print(part1 + '\ttrip' + part2)
