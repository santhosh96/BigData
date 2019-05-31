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

--Averaging out star ratings for every movie and also counting the respective counts of ratings
averageCountRatings = FOREACH ratingsByMovie GENERATE group AS movieID, 
			 		  COUNT(ratings.rating) AS countRating,
                      AVG(ratings.rating) AS avgRating;
DESCRIBE averageCountRatings

--Fliltering out the movies with average rating less than 2.0
worstStarMovies = FILTER averageCountRatings BY avgRating < 2.0;
DESCRIBE worstStarMovies;

--Joining the WorstStarMovies and nameLookup relation for finding the name of the movie corresponding to its ID
worstStarsWithData = JOIN worstStarMovies BY movieID, nameLookup BY movieID;
DESCRIBE worstStarsWithData

--Sorting according to the count of ratings
worstMoviesEver = ORDER worstStarsWithData BY worstStarMovies::countRating DESC;

DUMP worstMoviesEver;