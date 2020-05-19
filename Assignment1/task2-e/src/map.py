#!/usr/bin/env python

import sys

for line in sys.stdin:
    key, value = line.strip().split('\t')

    # get each taxi
    medallion = key.strip().split(',')[0]

    # get each day
    full_date = key.strip().split(',')[-1]
    day = full_date.strip().split(' ')[0]

    print (medallion + ',' + day)
