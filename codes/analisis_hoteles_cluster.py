from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import IDF, Tokenizer, RegexTokenizer, StopWordsRemover, CountVectorizer, NGram
from pyspark.ml.classification import NaiveBayes
import string
import sys
import re
import math

# necesario inicializar a parametro de entrada del programa  argv[1] 
if (sys.argv[1]):
	hotel_Name = sys.argv[1]   
#else:
#hotel_Name = "Hotel Arena"
# necesario inicializar a parametro de entrada del programa  argv[2]
if (sys.argv[2]):
	distancia = sys.argv[2]
#else:
#distancia = 100

conf = SparkConf().setAppName('Hoteles.py')  #cambiar por nombre de app
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

reviewsdf = spark.read.option("header",True).option("inferSchema","true").csv("Hotel_Reviews_Large.csv")

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

# busca la primera fila del hotel y obtiene coordenadas del centro del circulo
hotelFirstRow = reviewsdf.filter(col("Hotel_Name") == hotel_Name).first()
latitudectr = float (hotelFirstRow[-2])
longitudectr = float(hotelFirstRow[-1])

# filtramos los hoteles en el area

reviewsrdd = reviewsdf.rdd
filteredrdd = reviewsrdd.filter(lambda x: HaversineDistance(latitudectr, float(x[-2]), longitudectr, float(x[-1])) <= distancia)
filteredrdd.persist()
filtereddf = filteredrdd.toDF()


def mlib_naivebayeclf_ngram(input_array):

	# separar las palabras
	tokenizer = RegexTokenizer(inputCol="sentence", outputCol="words", pattern="\\W")
	wordsData = tokenizer.transform(sentenceData)

	# quitar las palabras consideradas como stop words
	remover = StopWordsRemover(inputCol="words", outputCol="filtered")
	wordsData = remover.transform(wordsData)

	# bigramas, seleccionar las palabras de 2 en 2
	ngram = NGram(n=2, inputCol="filtered", outputCol="ngrams")
	ngramData = ngram.transform(wordsData)

	# contar las palabras
	cv = CountVectorizer(inputCol="ngrams", outputCol="rawFeatures", minDF=2.0)
	cvModel = cv.fit(ngramData)
	featurizedData = cvModel.transform(ngramData)

	# calcularas las caracteristicas idf
	idf = IDF(inputCol="rawFeatures", outputCol="features")
	idfModel = idf.fit(featurizedData)
	rescaledData = idfModel.transform(featurizedData)

	# datos a entrenar
	train = rescaledData.select(['label', 'features'])

	# entrenar con naiveBayes
	nb = NaiveBayes(smoothing=1.0, modelType="multinomial")
	nbModel = nb.fit(train)

	# seleccionar el vocabulario y sus probabilidades de clasificacion para cada clase
	array = [i for i in zip(map(str, cvModel.vocabulary), map(float, nbModel.theta.toArray()[0]), map(float, nbModel.theta.toArray()[1]))]
	fields = [StructField('words', StringType(), True),
			StructField('negative', FloatType(), True),
			StructField('positive', FloatType(), True)  
	]
	schema = StructType(fields)
	output_df = spark.createDataFrame(array, schema = schema)

	return output_df

# igual que mlib_naivebayeclf_ngram solo que sin bigramas
def mlib_naivebayeclf(input_array):

	tokenizer = RegexTokenizer(inputCol="sentence", outputCol="words", pattern="\\W")
	wordsData = tokenizer.transform(sentenceData)

	remover = StopWordsRemover(inputCol="words", outputCol="filtered")
	wordsData = remover.transform(wordsData)

	cv = CountVectorizer(inputCol="filtered", outputCol="rawFeatures", minDF=2.0)
	cvModel = cv.fit(wordsData)
	featurizedData = cvModel.transform(wordsData)

	idf = IDF(inputCol="rawFeatures", outputCol="features")
	idfModel = idf.fit(featurizedData)
	rescaledData = idfModel.transform(featurizedData)

	train = rescaledData.select(['label', 'features'])

	nb = NaiveBayes(smoothing=1.0, modelType="multinomial")
	nbModel = nb.fit(train)

	array = [i for i in zip(map(str, cvModel.vocabulary), map(float, nbModel.theta.toArray()[0]), map(float, nbModel.theta.toArray()[1]))]
	fields = [StructField('words', StringType(), True),
			StructField('negative', FloatType(), True),
			StructField('positive', FloatType(), True)  
	]
	schema = StructType(fields)
	output_df = spark.createDataFrame(array, schema = schema)

	return output_df

# Patron 1
filtereddf.createOrReplaceTempView("temp")
ret  = spark.sql("select Hotel_Address,Hotel_Name,lat,lng,Average_Score,avg(Reviewer_Score),avg(Total_Number_of_Reviews_Reviewer_Has_Given),Reviewer_Nationality,count(Reviewer_Nationality) as Num_Client_of_Nationality from temp group by Hotel_Address,Reviewer_Nationality,Hotel_Name,lat,lng,Average_Score order by Hotel_Name ASC").coalesce(1).write.format("csv").option("header","true").save(hotel_Name + "_" + str(distancia))
#preguntar
nationalityrdd = filtereddf.select("Reviewer_Nationality").rdd.map(lambda x: x[0]).map(lambda x: (str(x).strip(), 1)).reduceByKey(lambda x,y : x+y).sortBy(lambda x: x[1], False)
nationalityrdd.coalesce(1).toDF().withColumnRenamed("_1", "Reviewer Nationality").withColumnRenamed("_2", "Count").write.format("csv").save("patron1.csv")



# Patron 2 y Patron 3
# seleccion de columnas y filtrado
positive_revw = filtereddf.select("Positive_Review").filter(col("Positive_Review") != "No Positive")
negative_revw = filtereddf.select("Negative_Review").filter(col("Negative_Review") != "No Negative")
# anyadir el grupo que pertenece para la clasificacion
negative_revw = negative_revw.withColumn("label", lit(0.0)).withColumnRenamed("Negative_Review", "sentence")
positive_revw = positive_revw.withColumn("label", lit(1.0)).withColumnRenamed("Positive_Review", "sentence")

sentenceData = positive_revw.union(negative_revw)

df_ngram = mlib_naivebayeclf_ngram(sentenceData)

df_ngram.sort(desc('positive')).select("words").write.format("csv").save("patron2.csv")

df_ngram.sort(desc('negative')).select("words").write.format("csv").save("patron3.csv")

# Patron 4: Aspectos importantes competencias a su alrededor
# filtrado
hotelAvgRating = float(hotelFirstRow[3])
higherRatingdf = filtereddf.filter(col("Average_Score") > hotelAvgRating)

positive_revw = higherRatingdf.select("Positive_Review").filter(col("Positive_Review") != "No Positive")
negative_revw = higherRatingdf.select("Negative_Review").filter(col("Negative_Review") != "No Negative")
negative_revw = negative_revw.withColumn("label", lit(0.0)).withColumnRenamed("Negative_Review", "sentence")
positive_revw = positive_revw.withColumn("label", lit(1.0)).withColumnRenamed("Positive_Review", "sentence")

sentenceData = positive_revw.union(negative_revw)

df=mlib_naivebayeclf(sentenceData)

df.sort(desc('positive')).select("words").write.format("csv").save("patron4.csv")

#Patron 5
tagsrdd = filtereddf.withColumn("Tags", regexp_replace(col("Tags"), "[\[\]']", "")).select("Tags").rdd.map(lambda x: x[0]).flatMap(lambda line: line.split(",")).map(lambda x: (str(x.lower()).strip().capitalize(), 1)).reduceByKey(lambda x,y : x+y).sortBy(lambda x: x[1], False)
tagsrdd.coalesce(1).toDF().withColumnRenamed("_1", "Tags").withColumnRenamed("_2", "Count").write.format("csv").save("patron5.csv")
