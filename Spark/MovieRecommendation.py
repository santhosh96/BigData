# Use export SPARK_MAJOR_VERSION=2 before running the code

from pyspark.sql import SparkSession
# ALS is a recommendation algorithm in mllib
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql.functions import lit

# Creating a dictionary for mapping the ID to names of the movies
def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.items") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1].decode('ascii', 'ignore')
    return movieNames

def parseInput(line):
    fields = line.value.split()
    # (userID, movieID, rating)
    return Row(userID = int(fields[0]), movieID = int(fields[1]), rating = float(fields[2]))

if __name__ == "__main__":
    
    # Creating a spark session
    spark = SparkSession.builder.appName("MovieRecs").getOrCreate()

    # creating a dictionary that creates mapping of movie ID to movie name
    movieNames = loadMovieNames()

    # Loading the raw data, converting the dataframe to rdd
    lines = spark.read.text("hdfs:///user/maria_dev/ml-100k/u.data").rdd

    # Converting the lines to RDD of row of objects with (userID, movieID, rating)
    ratingsRDD = lines.map(parseInput)

    # Converting the RDD into DataFrame, caching is done so that the data might be useful for future
    ratings = spark.createDataFrame(ratingsRDD).cache()

    # Creating the recommendation model:
    # Now, we will be creating a model using ALS for training on the movie lens data set where the
    # independent variables are UserID, MovieID and dependent variable is rating

    als = ALS(maxIter=5, regParam=0.01, userCol="userID", itemCol="movieID", ratingCol="rating")
    model = als.fit(ratings)

    # Displaying the ratings of the user 0:
    print("\nRatings of user 0:")
    userRatings = ratings.filter("userID = 0")
    for rating in userRatings.collect():
        print(movieNames[rating["movieID"]], rating["rating"])

    # Creating the test data:
    # Choosing only those movies that have been rated more than 100 times
    ratingCounts = ratings.groupBy("movieID").count().filter("count > 100")
    # Now adding a column of the desired user ID and creating the test dataset
    recommendMovies = ratingCounts.select("movieID").withColumn("userID", lit(0))

    # Predictions of ratings for the user for all the movies in the test data
    recommendation = model.transform(recommendMovies)

    # Fetching the top 20 movies
    topRecommendation = recommendation.sort(recommendation.prediction.desc()).take(20)

    # Displaying the recommendations
    print("\nTop 20 movie recommendations:")
    
    for movie in topRecommendation:
        print(movieNames[movie["movieID"]], movie["prediction"])

    spark.stop()

     



