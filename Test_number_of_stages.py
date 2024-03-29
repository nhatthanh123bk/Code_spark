from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType

spark = SparkSession\
    .builder\
    .appName("Test_number_of_stages")\
    .getOrCreate()

schema_ratings = StructType([
			    StructField("userId",IntegerType()),\
			    StructField("movieId", IntegerType()),\
			    StructField("rating", DoubleType()),\
			    StructField("timestamp",StringType())    
				])   

schema_movies = StructType([
				StructField("ID_movie",IntegerType()),\
				StructField("Name_movie",StringType()),\
				StructField("Stype_movie",StringType())
				])
# stage load data
df_ratings = spark.read.format("csv").schema(schema_ratings).load("/nhatthanh/data/ml-20m/ratings.csv")
df_movies = spark.read.format("csv").schema(schema_movies).load("/nhatthanh/data/ml-20m/movies.csv")

df_ratings.show()
df_movies.show()
# stage join
# ra = df_ratings.alias('ra')
# mo = df_movies.alias('mo')
# ratings_movies = ra.join(mo, ra.movieId == mo.ID_movie)
# ratings_movies.show()


