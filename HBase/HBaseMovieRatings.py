# Command for setting up backend REST service : /usr/hdp/current/hbase-master/bin/hbase-daemon.sh start rest -p 8000 --infoport 8001

# REST client for HBase with a python wrapper
import starbase
from starbase import Connection

# Creating a connection object to th REST service on HBase
c = Connection("127.0.0.1", "8000")

# Creating the table
ratings = c.table('ratings')

if (ratings.exists()): 
    print("\nDropping existing ratings table\n")
    ratings.drop()

# Creating the column family rating
ratings.create('rating')

# Reading the u.data file from local
print("\nParsing the ml-100k ratings data...\n")
ratingFile = open("/Users/maverick/work/BigDataUdemy/ml-100k/u.data", "r")

# Reading the data in batches for efficiency (provided by starbase)
batch = ratings.batch()

for line in ratingFile:
    # storing the values in tuple
    (userID, movieID, rating, timestamp) = line.split()
    
    # userID is the key for the row which has the column family rating, where we store the column movieID and corresponding rating value
    batch.update(userID, {'rating': {movieID: rating}})

ratingFile.close()

print ("\nCommitting ratings data to HBase via REST service\n")
batch.commit(finalize=True)

print ("\nGet back ratings for some users...\n")
print ("\nRatings for user ID 1:\n")
print (ratings.fetch("1"))
print ("\nRatings for user ID 33:\n")
print (ratings.fetch("33"))
print ("\nRatings for user ID 100:\n")
print (ratings.fetch("100"))

ratings.drop()
