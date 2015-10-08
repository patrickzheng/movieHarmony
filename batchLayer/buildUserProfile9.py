
# coding: utf-8

# In[1]:

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
#df = sqlContext.read.json("s3n://patricks3db/reviews_Movies_and_TV_small.json")
df = sqlContext.read.json("s3n://patricks3db/reviews_Movies_and_TV.json")
df.printSchema()


# In[19]:

ratings = df
ratings = ratings.drop("helpful")
ratings = ratings.drop("reviewText")
ratings = ratings.drop("reviewTime")
#ratings = ratings.drop("reviewerName")
ratings = ratings.drop("summary")
ratings = ratings.drop("unixReviewTime")
ratings.columns


# In[20]:

ratings.persist(StorageLevel.MEMORY_AND_DISK_SER)


# In[21]:

newUserDf = sqlContext.read.json("s3n://patricks3db/mapuidreviewerid/uidreviwerid.json")


# In[22]:

newUserDf.take(1)


# In[23]:

newMovieDf = sqlContext.read.json("s3n://patricks3db/mapmidasin/midasin.json")


# In[24]:

newMovieDf.take(1)


# In[25]:

newMovieDf.persist(StorageLevel.MEMORY_AND_DISK_SER)
newUserDf.persist(StorageLevel.MEMORY_AND_DISK_SER)


# In[26]:

rawRatings1 = ratings.join(newUserDf, ratings.reviewerID == newUserDf.reviewerid)
rawRatings1.printSchema()


# In[27]:

rawRatings2 = rawRatings1.join(newMovieDf, ratings.asin == newMovieDf.asin)
rawRatings2.printSchema()


# In[28]:

#print rawRatings2.take(1)[0].asin


# In[29]:

#rawRatings3 = rawRatings2.map(lambda row: (row.uid, row.mid, row.overall)).persist(StorageLevel.MEMORY_AND_DISK_SER)


# In[34]:

reviewerSummaryRDD1 = rawRatings2.map(lambda row: ((row.uid, row.reviewerID, row.reviewerName), {(row.asin, row.overall), }))


# In[35]:

reviewerSummaryRDD2 = reviewerSummaryRDD1.reduceByKey(lambda x, y: x | y)


# In[36]:

reviewerSummaryRDD3 = reviewerSummaryRDD2.map(lambda x: {"uid":x[0][0], "reviewerid":x[0][1], "reviewername":x[0][2], "numofreviews":len(x[1]), "ratings":{item[0]:item[1] for item in x[1]}})


# In[37]:

reviewerSummaryRDD1.take(1)


# In[38]:

def syncToCassandra(d_iter):
        from cqlengine import columns
        from cqlengine.models import Model
        from cqlengine import connection
        from cqlengine.management import sync_table
        CASSANDRA_KEYSPACE = "playground"
        class userprofile9(Model):
                uid = columns.Integer(primary_key=True)
                reviewerid = columns.Text()
                reviewername = columns.Text()
                numofreviews = columns.Float()
                ratings = columns.Map(columns.Text, columns.Float)
        connection.setup(['172.31.39.226'], CASSANDRA_KEYSPACE)
        sync_table(userprofile9)
        for d in d_iter:
                userprofile9.create(**d)
# Create table if it does not exist. Need to do this before submitting to Spark to avoid collisions
syncToCassandra([])
reviewerSummaryRDD3.foreachPartition(syncToCassandra)


# In[ ]:



