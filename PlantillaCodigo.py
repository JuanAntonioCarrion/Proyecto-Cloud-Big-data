from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import IDF, Tokenizer, RegexTokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.classification import NaiveBayes
import string
import sys
import re
import math
import numpy as np
import pandas as pd

conf = SparkConf().setMaster('local[*]').setAppName('plantilla.py')  #cambiar por nombre de app
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

reviewsdf = spark.read.option("header",True).option("inferSchema","true").csv("Hotel_Reviews.csv")

reviewsdf = reviewsdf.filter((col("lat") != "NA") & (col("lng")!= "NA"))  # Eliminar las reviews con coordenadas invalidas

# Function to calculate the distance given the earth radius and two pairs latitude/longitude
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


# necesario inicializar a parametro de entrada del programa  argv[1] 
hotel_Name = sys.argv[1]   
#hotel_Name = "Hotel Arena"
# necesario inicializar a parametro de entrada del programa  argv[2]
distancia = sys.argv[2]
#distancia = 100
# busca la primera fila del hotel y obtiene coordenadas del centro del circulo
hotelFirstRow = reviewsdf.filter(col("Hotel_Name") == hotel_Name).first()
latitudectr = float (hotelFirstRow[-2])
longitudectr = float(hotelFirstRow[-1])

# filtramos los hoteles en el area

reviewsrdd = reviewsdf.rdd

filteredrdd = reviewsrdd.filter(lambda x: HaversineDistance(latitudectr, float(x[-2]), longitudectr, float(x[-1])) <= distancia)

filtereddf = filteredrdd.toDF()



# Patron 1
filtereddf.createOrReplaceTempView("temp")
ret  = spark.sql("select Hotel_Address,Hotel_Name,lat,lng,Average_Score,avg(Reviewer_Score),avg(Total_Number_of_Reviews_Reviewer_Has_Given),Reviewer_Nationality,count(Reviewer_Nationality) as Num_Client_of_Nationality from temp group by Hotel_Address,Reviewer_Nationality,Hotel_Name,lat,lng,Average_Score order by Hotel_Name ASC").coalesce(1).write.format("csv").option("header","true").save(hotel_Name + " " + str(distancia))



# Patron 2 y Patron 3
positive_revw = filtereddf.select("Positive_Review").filter(col("Positive_Review") != "No Positive")
negative_revw = filtereddf.select("Negative_Review").filter(col("Negative_Review") != "No Negative")
negative_revw = negative_revw.withColumn("label", lit(0.0)).withColumnRenamed("Negative", "sentence")
positive_revw = positive_revw.withColumn("label", lit(1.0)).withColumnRenamed("Positive", "sentence")

sentenceData = positive_revw.union(negative_revw)

tokenizer = RegexTokenizer(inputCol="sentence", outputCol="words", pattern="\\W")
wordsData = tokenizer.transform(sentenceData)

remover = StopWordsRemover(inputCol="words", outputCol="filtered")
wordsData = remover.transform(wordsData)

cv = CountVectorizer(inputCol="filtered", outputCol="rawFeatures", minDF=1.0)
cvModel = cv.fit(wordsData)
featurizedData = cvModel.transform(wordsData)

idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurizedData)
rescaledData = idfModel.transform(featurizedData)

train = rescaledData.select(['label', 'features'])

nb = NaiveBayes(smoothing=1.0, modelType="multinomial")
nbModel = nb.fit(train)

array = np.asarray(zip(cvModel.vocabulary, nbModel.theta.toArray()[0], nbModel.theta.toArray()[1]))

df = pd.DataFrame(array, columns=["words", "negative", "positive"])
df[["negative", "positive"]] = df[["negative", "positive"]].astype(float)

df.sort_values(by=['positive'], inplace=True, ascending = False)
result_positive = df['words'][:20]
result_positive.to_csv('relevantes_positive.csv', header=False, index=False)

df.sort_values(by=['negative'], inplace=True, ascending = False)
result_positive = df['words'][:20]
result_positive.to_csv('relevantes_negative.csv', header=False, index=False)


# Patron 4: Aspectos importantes competencias a su alrededor
# filtrado
hotelAvgRating = float(hotelFirstRow[3])
higherRatingdf = filtereddf.filter(col("Average_Score") > hotelAvgRating)

#word counter o patron 2
#wordsrdd = higherRatingdf.select("Positive_Review").rdd.map(lambda x: x[0]).flatMap(lambda line: line.split()).map(lambda x: (str(x.lower()), 1)).reduceByKey(lambda x,y : x+y).sortBy(lambda x: x[1], False)
#wordsrdd.saveAsTextFile("patron4.txt")


#Patron 5

tagsdf = higherRatingdf.select("Hotel_Name","Tags","Average_Score").rdd.map(lambda x: x[0]).flatMap(lambda line: line.split()).map(lambda x: (str(x.lower()), 1)).reduceByKey(lambda x,y : x+y).groupBy(higherRatingdf.Hotel_Name)
tagsdf.toDF().coalesce(1).write.format("csv").option("header","true").save(hotel_Name + " TAGS " + str(distancia))



