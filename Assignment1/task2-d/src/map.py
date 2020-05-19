#!/usr/bin/env python

import sys

for line in sys.stdin:
    key, value = line.strip().split('\t')
    
    # get day as key
    full_date = key.strip().split(',')[-1]
    day = full_date.strip().split(' ')[0]
    
    # get fare, tips, surcharge
    fare = float(value.strip().split(',')[-6])
    tips = float(value.strip().split(',')[-3])
    surcharge = float(value.strip().split(',')[-5])
    final_revenue = str(fare + tips + surcharge)
    
    # get tolls
    toll = float(value.strip().split(',')[-2])
    final_toll = str(toll)

    print(day + '\t' + final_revenue + ',' + final_toll)