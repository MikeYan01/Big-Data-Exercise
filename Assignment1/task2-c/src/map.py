#!/usr/bin/env python

import sys

for line in sys.stdin:

    # get trip value part
    trip_part = line.strip().split('\t')[1]
    passenger_count = trip_part.strip().split(',')[3]
    
    print (passenger_count + '\t' + '1')