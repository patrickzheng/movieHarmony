
# coding: utf-8

# In[1]:

from pyspark.sql import SQLContext
from pyspark import SparkContext
sc = SparkContext()
sqlContext = SQLContext(sc)
df = sqlContext.read.json("s3n://patricks3db/reviews_Movies_and_TV_small.json")
df.printSchema()


# In[2]:

ratings = df
ratings = ratings.drop("helpful")
ratings = ratings.drop("reviewText")
ratings = ratings.drop("reviewTime")
ratings = ratings.drop("reviewerName")
ratings = ratings.drop("summary")
ratings = ratings.drop("unixReviewTime")
ratings.columns


# In[31]:

users = ratings.map(lambda r: r.reviewerID).distinct().zipWithIndex()
users.take(5)
users.count()


# In[32]:

movies = ratings.map(lambda r: r.asin).distinct().zipWithIndex()
movies.take(5)
movies.count()


# In[5]:

moviesDf = sqlContext.createDataFrame(movies, ['asin', 'movieID'])
moviesDf.take(5)
usersDf = sqlContext.createDataFrame(users, ['reviewerID', 'userID'])
usersDf.take(5)


# In[6]:

rawRatings1 = ratings.join(usersDf, ratings.reviewerID == usersDf.reviewerID).drop("reviewerID")


# In[7]:

rawRatings2 = rawRatings1.join(moviesDf, ratings.asin == moviesDf.asin).drop("asin")


# In[9]:

rawRatings3 = rawRatings2.map(lambda row: (row.userID, row.movieID, row.overall)).cache()


# In[10]:

from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating


# In[11]:

rank = 10
numIterations = 10
model = ALS.train(rawRatings3, rank, numIterations)


# In[41]:

userMoviePair = usersDf.join(moviesDf).drop('reviewerID').drop('asin')
userMoviePair.printSchema()
#userMoviePair = userMoviePair.filter(userMoviePair.userID < 10000)
userMoviePair = userMoviePair.map(lambda row: (row.userID, row.movieID))


# In[44]:

recommendations = model.predictAll(userMoviePair)


# In[45]:

recommendationsToC = recommendations.map(lambda x: {"user":x[0], "product":x[1], "rating":x[2]})


# In[51]:

def AddToCassandra_allcountsbatch_bypartition(d_iter):
        from cqlengine import columns
        from cqlengine.models import Model
        from cqlengine import connection
        from cqlengine.management import sync_table
        CASSANDRA_KEYSPACE = "playground"
        class reviewerProfile(Model):
                user = columns.Integer(primary_key=True)
                product = columns.Integer(primary_key=True)
                rating = columns.Float(primary_key=True, clustering_order="DESC")
                
        connection.setup(['172.31.39.226'], CASSANDRA_KEYSPACE)
        sync_table(reviewerProfile)
        for d in d_iter:
                reviewerProfile.create(**d)

# Create table if it does not exist. Need to do this before submitting to Spark to avoid collisions
AddToCassandra_allcountsbatch_bypartition([])
recommendationsToC.foreachPartition(AddToCassandra_allcountsbatch_bypartition)


# In[ ]:




