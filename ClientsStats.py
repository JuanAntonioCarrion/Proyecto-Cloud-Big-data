from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,substring, length, col, expr
import string
import sys

spark=SparkSession.builder.master("local[*]").appName("rating").getOrCreate()

df=spark.read.option("header","true").option("inferSchema","true").csv("Hotel_Reviews.csv")

df.createOrReplaceTempView("temp")
ret  = spark.sql("select Hotel_Name,lat,lng,Average_Score,avg(Reviewer_Score),avg(Total_Number_of_Reviews_Reviewer_Has_Given),Reviewer_Nationality from temp group by Reviewer_Nationality,Hotel_Name,lat,lng,Average_Score order by Hotel_Name ASC").write.format("csv").option("header","true").save("clientsNatonalityReview")
