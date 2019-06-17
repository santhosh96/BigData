from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.items") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

def parseInput(line):
    fields = line.split()
    # (movieID, (ratings, 1.0))
    return Row(movieID = int(fields[1]), rating = float(fields[2]))

if __name__ == "__main__":

    # Create a Spark Session
    # getOrCreate() either creates a new session or gets the older session that was not stopped previously
    spark = SparkSession.builder.enableHiveSupport().appName("WorstMovies").getOrCreate()
    
    # creating a dictionary that creates mapping of movie ID to movie name
    movieNames = loadMovieNames()

    # get the raw data
    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/u.data")
    # converting the data into RDD of row objects (movieID, rating)
    movies = lines.map(parseInput)
    # converting the row objects into DataFrame
    movieDataset = spark.createDataFrame(movies)

    # computing the average of the ratings of each movie ID
    averageRatings = movieDataset.groupBy("movieID").avg("rating")

    # computing the counts of ratings for each movie ID
    counts = movieDataset.groupBy("movieID").count()

    # only choosing those ratings that are being rated by more than 10 people
    counts = counts.filter("count > 10")

    # joining both the averageRatings and counts dataset over movieID
    averageAndCounts = counts.join(averageRatings, "movieID")

    # taking the top 10 results
    results = averageAndCounts.orderBy("avg(rating)").take(10)

    # Display the results, by mapping the movieID to movie name
    for res in results:
        print(movieNames[res[0]], res[1], res[2])

    # Stopping the Spark session
    spark.stop()