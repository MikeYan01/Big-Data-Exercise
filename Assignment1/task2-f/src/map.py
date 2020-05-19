#!/usr/bin/env python

import sys

for line in sys.stdin:
    key = line.strip().split('\t')[0]

    # get medallion
    medallion = key.strip().split(',')[0]

    # get license
    license = key.strip().split(',')[1]

    print(license + ',' + medallion)