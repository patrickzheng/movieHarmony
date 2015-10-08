
# coding: utf-8

# In[1]:

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
#df = sqlContext.read.json("s3n://patricks3db/reviews_Movies_and_TV_small.json")
df = sqlContext.read.json("s3n://patricks3db/reviews_Movies_and_TV.json")
df.printSchema()


# In[2]:

# check mrubash1/Origin for downloading and updating to s3


# In[3]:

ratings = df
ratings = ratings.drop("helpful")
ratings = ratings.drop("reviewText")
ratings = ratings.drop("reviewTime")
#ratings = ratings.drop("reviewerName")
ratings = ratings.drop("summary")
ratings = ratings.drop("unixReviewTime")
ratings.columns


# In[4]:

ratings


# In[5]:

#numRatings = ratings.count()
#numRatings


# In[6]:

users = ratings.map(lambda r: r.reviewerID).distinct().zipWithIndex()
#users.take(5)


# In[7]:

#numUsers = users.count()
#print numUsers


# In[8]:

movies = ratings.map(lambda r: r.asin).distinct().zipWithIndex()


# In[9]:

movies.take(1)


# In[10]:

moviesDf = sqlContext.createDataFrame(movies, ['asin', 'mid'])


# In[11]:

moviesDf.toJSON().take(1)


# In[12]:

moviesDf.toJSON().saveAsTextFile("s3n://patricks3db/mapmidasin/midasin.json")


# In[13]:

newMovieDf = sqlContext.read.json("s3n://patricks3db/mapmidasin/midasin.json")


# In[14]:

#newMovieDf.take(1)


# In[15]:

users = ratings.map(lambda r: r.reviewerID).distinct().zipWithIndex()
usersDf = sqlContext.createDataFrame(users, ['reviewerid', 'uid'])
usersDf.toJSON().saveAsTextFile("s3n://patricks3db/mapuidreviewerid/uidreviwerid.json")


# In[16]:

newUserDf = sqlContext.read.json("s3n://patricks3db/mapuidreviewerid/uidreviwerid.json")
#newUserDf.take(1)


# In[17]:

newUserDf.printSchema()
newMovieDf.printSchema()


# In[18]:

#moviesDf = sqlContext.createDataFrame(movies, ['asin', 'mid'])
#print moviesDf.take(5)
#print type(movies)


# In[19]:

#usersDf = sqlContext.createDataFrame(users, ['reviewerID', 'uid'])
#print usersDf.take(5)


# In[20]:

#rawRatings1 = ratings.join(usersDf, ratings.reviewerID == usersDf.reviewerID)
#rawRatings1.printSchema()


# In[21]:

#rawRatings2 = rawRatings1.join(moviesDf, ratings.asin == moviesDf.asin)
#rawRatings2.printSchema()


# In[22]:

#print rawRatings2.take(1)[0].asin


# In[23]:

#rawRatings3 = rawRatings2.map(lambda row: (row.uid, row.mid, row.overall)).persist(StorageLevel.MEMORY_AND_DISK_SER)


# In[24]:

#rawRatings3.take(10)


# In[25]:

#from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
#rank = 100
#numIterations = 10
#model = ALS.train(rawRatings3, rank, numIterations)


# In[26]:

#type(model)


# In[27]:

#type(ALS)


# In[28]:

#userMoviePair = usersDf.join(moviesDf).drop('reviewerID').drop('asin').map(lambda row: (row.userID, row.movieID))


# In[29]:

#userMoviePair.take(5)
#type(userMoviePair)


# In[30]:

#recommendations = model.predictAll(userMoviePair)


# In[31]:

#type(recommendations)


# In[32]:

#recommendations.take(5)


# In[ ]:



