
# coding: utf-8

# In[1]:

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
#df = sqlContext.read.json("s3n://patricks3db/reviews_Movies_and_TV_small.json")
df = sqlContext.read.json("s3n://patricks3db/reviews_Movies_and_TV.json")
df.printSchema()


# In[2]:

#df.take(5)


# In[3]:

ratings = df
ratings = ratings.drop("helpful")
ratings = ratings.drop("reviewText")
ratings = ratings.drop("reviewTime")
ratings = ratings.drop("reviewerName")
ratings = ratings.drop("summary")
ratings = ratings.drop("unixReviewTime")
ratings.columns


# In[4]:

ratings.persist(StorageLevel.MEMORY_AND_DISK_SER)


# In[5]:

newUserDf = sqlContext.read.json("s3n://patricks3db/mapuidreviewerid/uidreviwerid.json")


# In[6]:

newUserDf.take(1)


# In[7]:

newMovieDf = sqlContext.read.json("s3n://patricks3db/mapmidasin/midasin.json")


# In[8]:

newMovieDf.take(1)


# In[9]:

newMovieDf.persist(StorageLevel.MEMORY_AND_DISK_SER)
newUserDf.persist(StorageLevel.MEMORY_AND_DISK_SER)


# In[10]:

rawRatings1 = ratings.join(newUserDf, ratings.reviewerID == newUserDf.reviewerid)
rawRatings1.printSchema()


# In[11]:

rawRatings2 = rawRatings1.join(newMovieDf, ratings.asin == newMovieDf.asin)
rawRatings2.printSchema()


# In[12]:

#print rawRatings2.take(1)[0].asin


# In[13]:

rawRatings3 = rawRatings2.map(lambda row: (row.uid, row.mid, row.overall)).persist(StorageLevel.MEMORY_AND_DISK_SER)


# In[14]:

#rawRatings3.take(10)


# In[15]:

#ALS.extractParamMap()


# In[16]:

from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
rank = 200
numIterations = 20
#model = ALS.train(rawRatings3, rank, numIterations, 0.01)
#model = ALS.trainImplicit(rawRatings3, rank, numIterations, 0.01)
model = ALS.trainImplicit(rawRatings3, rank, numIterations, 0.03)


# In[17]:

model.recommendProducts(100000,5)


# In[18]:

#type(model)


# In[19]:

#type(ALS)


# In[20]:

#userMoviePair = usersDf.join(moviesDf).drop('reviewerID').drop('asin').map(lambda row: (row.userID, row.movieID))


# In[21]:

#userMoviePair.take(5)
#type(userMoviePair)


# In[22]:

#recommendations = model.predictAll(userMoviePair)


# In[23]:

#type(recommendations)


# In[24]:

#recommendations.take(5)


# In[25]:

path = "s3n://patricks3db/modelsComplete200r20i003a"
model.save(sc, path)


# In[26]:

sameModel = MatrixFactorizationModel.load(sc, path)


# In[27]:

sameModel.recommendProducts(1,5)


# In[28]:

sameModel.userFeatures().persist(StorageLevel.MEMORY_AND_DISK_SER)


# In[29]:

sameModel.recommendProducts(100000,5)


# In[30]:

sameModel.recommendProducts(500000,5)


# In[31]:

sameModel.recommendProducts(1000000,5)


# In[ ]:




# In[ ]:



