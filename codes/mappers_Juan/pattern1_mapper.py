#!/usr/bin/python

import sys
import re
import math

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

#Main function. Depending on the pattern it will map the parameters needed

def map_parameters(index_list, max_dist):

	index = 0
	detenido = False

	latitud_orig = float(str(sys.argv[1]))
	longitud_orig = float(str(sys.argv[2]))
	latitud_nueva = 0.0
	longitud_nueva = 0.0
	primera_linea = True
	no_tiene = False
	no_imprimas = False


	for line in sys.stdin:
		no_imprimas = False
		no_tiene = False
		output = ""
		if (not(primera_linea)):

			index = 0
			line_list = line.split(",")

			for iter_line in line_list:

				if (iter_line.startswith("\"[")):
					detenido = True

				if (iter_line.endswith("]\"")):
					detenido = False

				if (index == 15):
					if (iter_line == "NA"):
						no_tiene = True
					else:
						latitud_nueva = float(iter_line)	

				if (index != 16 and index != 15):
					if (index in index_list):
						output = output + iter_line + "\t"

				elif (index != 15):
					if (iter_line == "NA" or no_tiene):
						#output = "NA" + output
						no_imprimas = True
					else:
						longitud_nueva = float(iter_line)
						distancia = HaversineDistance(latitud_nueva, latitud_orig, longitud_nueva, longitud_orig)
						if ((float(max_dist) - distancia) < 0):
							no_imprimas = True
						#output = output + str(float(max_dist) - distancia)

				if(not(detenido)):
					index += 1
			if (not(no_imprimas)):
				output += "1"
				print(output)
		else:
			primera_linea = False

max_dist = str(sys.argv[3])
valid_pattern = True
index_list = [5] 

map_parameters(index_list, max_dist)
