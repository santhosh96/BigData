from mrjob.job import MRJob
from mrjob.step import MRStep

class MoviesPopularity(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_popularity)
        ]

    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    def reducer_count_popularity(self, movieID, rating_counts):
        yield movieID, sum(rating_counts)

if __name__ == '__main__':
    MoviesPopularity.run()
