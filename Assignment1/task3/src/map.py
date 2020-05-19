#!/usr/bin/env python

import sys

for line in sys.stdin:
    # skip license's first line
    if line.startswith('medallion'):
        continue
    else:
        # skip empty line
        if (len(line) <= 1):
            continue

        # decide where current line is from
        current_line = line.strip().split('\t')

        # from TripFareJoin
        if len(current_line) == 2:
            all_keys = current_line[0].strip().split(',')
            all_values = current_line[1].strip().split(',')

            # separate medallion from other attributes
            medallion = all_keys[0]
            other_keys = all_keys[1:]
            part1 = ','.join(other_keys)
            part2 = ','.join(all_values)
            print(medallion + '\ttf' + part1 + ',' + part2)
        
        # from licenses
        elif len(current_line) == 1:
            current_line = line.strip().split(',')

            # separate medallion from other attributes
            medallion = current_line[0]
            all_values = current_line[1:]
            part1 = ','.join(all_values)
            print(medallion + '\tlc' + part1)

