
# coding: utf-8

# In[59]:

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
df = sqlContext.read.json("s3n://patricks3db/reviews_Movies_and_TV.json")
df.printSchema()


# In[60]:

# check mrubash1/Origin for downloading and updating to s3


# In[61]:

ratings = df
ratings = ratings.drop("helpful")
ratings = ratings.drop("reviewText")
ratings = ratings.drop("reviewTime")
#ratings = ratings.drop("reviewerName")
#ratings = ratings.drop("summary")
ratings = ratings.drop("unixReviewTime")
ratings.columns


# In[62]:

ratings.persist()


# In[ ]:

numRatings = ratings.count()
numRatings


# In[ ]:

users = ratings.map(lambda r: r.reviewerID).distinct().zipWithIndex()
users.take(5)


# In[ ]:

numUsers = users.count()
print numUsers


# In[ ]:

users = ratings.map(lambda r: r.reviewerID).distinct().zipWithIndex()


# In[ ]:

usersDf = sqlContext.createDataFrame(users, ['reviewerID', 'uid'])
print usersDf.take(5)


# In[ ]:

rawRatings1 = ratings.join(usersDf, ratings.reviewerID == usersDf.reviewerID)
rawRatings1.printSchema()


# In[ ]:

rawRatings1.persist()


# In[ ]:

#print rawRatings1.take(1)[0].asin


# In[ ]:

reviewerSummaryRDD1 = rawRatings1.map(lambda row: ((row.uid, row.reviewerID, row.reviewerName), {(row.asin, row.overall), }))
reviewerSummaryRDD2 = reviewerSummaryRDD1.reduceByKey(lambda x, y: x | y)
reviewerSummaryRDD2.take(5)


# In[ ]:

reviewerSummaryRDD2.persist()


# In[ ]:




# In[ ]:

reviewerSummaryRDD3 = reviewerSummaryRDD2.map(lambda x: {"uid":x[0][0], "reviewerid":x[0][1], "reviewername":x[0][2], "numofreviews":len(x[1]), "ratings":{item[0]:item[1] for item in x[1]}})


# In[ ]:

def syncToCassandra(d_iter):
        from cqlengine import columns
        from cqlengine.models import Model
        from cqlengine import connection
        from cqlengine.management import sync_table
        CASSANDRA_KEYSPACE = "playground"
        class userprofile0(Model):
                uid = columns.Integer(primary_key=True)
                reviewerid = columns.Text()
                reviewername = columns.Text()
                numofreviews = columns.Float(primary_key=True, clustering_order="DESC")
                ratings = columns.Map(columns.Text, columns.Float)
        connection.setup(['172.31.39.226'], CASSANDRA_KEYSPACE)
        sync_table(userprofile0)
        for d in d_iter:
                userprofile0.create(**d)
# Create table if it does not exist. Need to do this before submitting to Spark to avoid collisions
syncToCassandra([])
reviewerSummaryRDD3.foreachPartition(syncToCassandra)


# In[ ]:



