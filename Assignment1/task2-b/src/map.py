#!/usr/bin/env python

import sys

for line in sys.stdin:
    # separate key and value, get total_amount
    key, value = line.strip().split('\t')
    total_amount = float(value.strip().split(',')[-1])

    if total_amount <= 10:
        print ('trip < $10')