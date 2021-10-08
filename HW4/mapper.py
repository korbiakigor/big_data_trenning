#!/usr/bin/env python

import sys

for line in sys.stdin:

    IATA_CODE = "-1"
    AIRLINE = "-1"
    DEPARTURE_DELAY = "-1"

    fields = line.strip().split(",")

    if fields[0] != "IATA_CODE" and fields[0] != "YEAR":
        if len(fields) == 2:
            IATA_CODE = fields[0]
            AIRLINE = fields[1]
        else:
            IATA_CODE = fields[4]
            DEPARTURE_DELAY = fields[11]

        print("{0}\t{1}\t{2}".format(IATA_CODE, AIRLINE, DEPARTURE_DELAY))
