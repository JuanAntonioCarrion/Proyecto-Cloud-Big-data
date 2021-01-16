#!/usr/bin/python

import sys
import re
import math
import string


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

#Function to calculate the distance given the earth radius and two pairs latitude/longitude
def HaversineDistance(lat1c, lat2c, lon1c, lon2c):
	
	lat1 = math.radians(lat1c)
	lat2 = math.radians(lat2c)
	lon1 = math.radians(lon1c)
	lon2 = math.radians(lon2c)
	R = 6373.0
	dlon = lon2 - lon1
	dlat = lat2 - lat1
	a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
	c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
	distance = R * c

	return distance


def map_parameters(latitud_orig, longitud_orig, max_dist):

	
	next(sys.stdin) #removing first line

	for line in sys.stdin:
		line_list = line.split(",")
		latitud_nueva = line_list[-2].strip()
		longitud_nueva = line_list[-1].strip()
		comment = line_list[6].strip()
	
		if(is_number(latitud_nueva) and is_number(longitud_nueva) and comment!=""):
			distancia = HaversineDistance(float(latitud_nueva), latitud_orig, float(longitud_nueva), longitud_orig)
			if (max_dist - distancia >= 0):
				comment_list = comment.split(" ")
				for word in comment_list:
					if(word.strip()!=""):
						print word.capitalize() + "\t" + "1"
						
			
latitud_orig = float(sys.argv[1])
longitud_orig = float(sys.argv[2])
max_dist = float(sys.argv[3])


map_parameters(latitud_orig,longitud_orig,max_dist)
