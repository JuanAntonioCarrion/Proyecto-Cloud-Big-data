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


	for line in sys.stdin:
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
						output = output + "\t" + iter_line

				elif (index != 15):
					if (iter_line == "NA" or no_tiene):
						output = "NA" + output
					else:
						longitud_nueva = float(iter_line)
						distancia = HaversineDistance(latitud_nueva, latitud_orig, longitud_nueva, longitud_orig)
						output = str(float(max_dist) - distancia) + output

				if(not(detenido)):
					index += 1

			print(output)
		else:
			primera_linea = False

index_list = []
mode = str(sys.argv[3])
max_dist = str(sys.argv[4])
valid_pattern = True
if (mode == "pattern1"): #Solo nos interesa la nacionalidad del reviewer
	index_list = [5] 
elif (mode == "pattern2"): #Nos interesa el positive review y el total de palabras positivas
	index_list = [9,10]
elif (mode == "pattern3"): #Nos interesa el negative review y el total de palabras negativas
	index_list = [6,7]
elif (mode == "pattern4"): #Nos interesa el nombre del hotel, la avg score, el positive review y eltotal de palabras positivas
	index_list = [3,4,6,7]
elif (mode == "pattern5"): #Nos interesan las tags 
	index_list = [13]
else:
	valid_pattern = False
	print("Unknown pattern")

if (valid_pattern):
	map_parameters(index_list, max_dist)

"""In order to make the code more versatile, we include a 1 at the end
so that we give the reducer the posibility of making a count of the appearances
of the word"""