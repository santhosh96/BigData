# importing necessary packages for creating RDDs and using them
from pyspark import SparkConf, SparkContext

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
    return (int(fields[1]), (float(fields[2]), 1.0))

if __name__ == "__main__":
    
    # Creating the object of the configuration class that sets the name of the application
    # as "WorstMovies", this would set the name of the job that can viewed under the dashboard
    conf = SparkConf().setAppName("WorstMovies")
    # sc is the variable for indicating the environment we are running within
    sc = SparkContext(conf=conf)

    # creating a dictionary that creates mapping of movie ID to movie name
    movieNames = loadMovieNames()

    # Loading the text file of u.data to a RDD from HDFS
    # Text file contains rows of user id, a movie id, rating, and time stamp.
    lines = sc.textFile("hdfs:///user/maria_dev/ml-100k/u.data")

    # Convert the text into tuples of structure (movieID, (ratings, 1.0))
    # Apply parseInput to all the lines of RDD lines and load into movieRatings RDD
    movieRatings = lines.map(parseInput)

    # Reducing to (movieID, (total_ratings, total_rating_counts))
    # Summing up the ratings and number of times the ratings was done
    # For a unique movie ID, take 2 tuples mov1 and mov2 and sum the ratings and rating counts
    ratingTotalsAndCount = movieRatings.reduceByKey(lambda mov1, mov2: (mov1[0] + mov2[0], mov1[1] + mov2[1]))

    # Mapping to (movieID, averageRatings)
    # for each movieID, divide the vale 1 by value 2
    averageRatings = ratingTotalsAndCount.mapValues(lambda movieavgrating : movieavgrating[0] / movieavgrating[1])

    # Sorting the averageratings in increasing order of the averaRatings field
    sortAverageRatings = averageRatings.sortBy(lambda x: x[1])

    # Take the first 10 movies with the least average ratings
    results = sortAverageRatings.take(10)

    # Display the results, by mapping the movieID to movie name
    for res in results:
        print(movieNames[res[0]],res[1])