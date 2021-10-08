#!/usr/bin/env python

import sys

sum = 0
cnt = 0

for line in sys.stdin:

    line = line.strip().split('\t')
    
    code, airline, delay = line[0], line[1], float(line[2])

    if airline == "-1":
        sum += delay
        cnt += 1
    else:
        if cnt == 0:
            avg = 0
        else:
            avg = sum/cnt

        print("{0}\t{1}\t{2}".format(avg, code, airline))
        sum = 0
        cnt = 0
