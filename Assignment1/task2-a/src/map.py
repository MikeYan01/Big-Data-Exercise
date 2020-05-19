#!/usr/bin/env python

import sys

for line in sys.stdin:
    # separate key and value, get raw_fare
    key, value = line.strip().split('\t')
    raw_fare = float(value.strip().split(',')[11])
    final_fare = 0
    
    # decide which range the fare falls. final_fare is the start of each range
    if raw_fare >= 0:
        # deal with 0
        if raw_fare == 0:
            final_fare = 0

        # deal with fare > 48, modify to 48
        elif raw_fare > 48:
            final_fare = 48

        # fare is 4's multiple
        elif raw_fare % 4 == 0:
            final_fare = (int(raw_fare / 4) - 1) * 4
        
        else:
            final_fare = int(raw_fare / 4) * 4 
        
        if final_fare == 0:
            print ('a' + str(final_fare) + '\t' + '1')
        elif final_fare == 4:
            print ('b' + str(final_fare) + '\t' + '1')
        elif final_fare == 8:
            print ('c' + str(final_fare) + '\t' + '1')
        elif final_fare == 12:
            print ('d' + str(final_fare) + '\t' + '1')
        elif final_fare == 16:
            print ('e' + str(final_fare) + '\t' + '1')
        elif final_fare == 20:
            print ('f' + str(final_fare) + '\t' + '1')
        elif final_fare == 24:
            print ('g' + str(final_fare) + '\t' + '1')
        elif final_fare == 28:
            print ('h' + str(final_fare) + '\t' + '1')
        elif final_fare == 32:
            print ('i' + str(final_fare) + '\t' + '1')
        elif final_fare == 36:
            print ('j' + str(final_fare) + '\t' + '1')
        elif final_fare == 40:
            print ('k' + str(final_fare) + '\t' + '1')
        elif final_fare == 44:
            print ('l' + str(final_fare) + '\t' + '1')
        elif final_fare == 48:
            print ('m' + str(final_fare) + '\t' + '1')
