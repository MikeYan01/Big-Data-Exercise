#!/usr/bin/env python

import sys

count = 0

# count all lines
for line in sys.stdin:
    count += 1

print(count)