#!/usr/bin/python

import sys
import re
import math

previous = None
counter = 0

for line in sys.stdin:
    key, value = line.split( "\t" )
    
    if key != previous:
        if previous is not None:
            print (previous+ ',' + str(counter))
        previous = key
        counter = 0
    
    counter += int(value)

print (previous+ ',' + str(counter))