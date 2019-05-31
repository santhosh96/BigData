--Loading the user rating data as relation named ratings
ratings = LOAD '/user/maria_dev/ml-100k/u.data' AS (userID:int, movieID:int, rating:int, ratingTime:int);
DESCRIBE ratings;

--Loading movie data as relation named metadata
metadata = LOAD '/user/maria_dev/ml-100k/u.item' USING PigStorage('|')
	AS (movieID:int, movieTitle:chararray, releaseDate:chararray, videoRelease:chararray, imdbLink:chararray);
    
--Extracting movie ID, title and the unix timestamp from the metadata relation
nameLookup = FOREACH metadata GENERATE movieID, movieTitle,
	ToUnixTime(ToDate(releaseDate, 'dd-MMM-yyyy')) AS releaseTime;
DESCRIBE nameLookup

--Grouping the ratings relation w.r.t movie ID
ratingsByMovie = GROUP ratings BY movieID;
DESCRIBE ratingsByMovie

--Averaging out star ratings for every movie
avgRatings = FOREACH ratingsByMovie GENERATE group AS movieID, AVG(ratings.rating) AS avgRating;
DESCRIBE avgRatings

--Fliltering out the movies with average rating more than 4
fiveStarMovies = FILTER avgRatings BY avgRating > 4.0;
DESCRIBE fiveStarMovies;

--Joining the highStarMovies and nameLookup relation for finding the name of the movie corresponding to its ID
fiveStarsWithData = JOIN fiveStarMovies BY movieID, nameLookup BY movieID;
DESCRIBE fiveStarsWithData

--Sorting according to the earliest timestamp
oldestFiveStarMovies = ORDER fiveStarsWithData BY nameLookup::releaseTime;

DUMP oldestFiveStarMovies;