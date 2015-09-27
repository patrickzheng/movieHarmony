
# coding: utf-8

# In[2]:

from cqlengine import columns
from cqlengine.models import Model
from cqlengine import connection
from cqlengine.management import sync_table
CASSANDRA_KEYSPACE = "playground"
connection.setup(['172.31.39.226'], CASSANDRA_KEYSPACE)
import json


# In[3]:

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
sc = SparkContext()
sqlContext = SQLContext(sc)
#df = sqlContext.read.json("s3n://patricks3db/reviews_Movies_and_TV_small.json")
df = sqlContext.read.json("s3n://patricks3db/reviews_Movies_and_TV.json")
df.printSchema()


# In[4]:

ratings = df
ratings = ratings.drop("helpful")
ratings = ratings.drop("reviewText")
ratings = ratings.drop("reviewTime")
ratings = ratings.drop("reviewerName")
ratings = ratings.drop("summary")
ratings = ratings.drop("unixReviewTime")
ratings.columns


# In[5]:

#users = ratings.map(lambda r: r.reviewerID).distinct().zipWithIndex()
#users.take(5)
#users.count()


# In[6]:

#movies = ratings.map(lambda r: r.asin).distinct().zipWithIndex()
#movies.take(5)
#movies.count()


# In[7]:

#moviesDf = sqlContext.createDataFrame(movies, ['asin', 'movieID'])
#moviesDf.take(5)
#usersDf = sqlContext.createDataFrame(users, ['reviewerID', 'userID'])
#usersDf.take(5)


# In[8]:

#rawRatings1 = ratings.join(usersDf, ratings.reviewerID == usersDf.reviewerID).drop("reviewerID")


# In[9]:

#rawRatings2 = rawRatings1.join(moviesDf, ratings.asin == moviesDf.asin).drop("asin")


# In[10]:

#rawRatings3 = rawRatings2.map(lambda row: (row.userID, row.movieID, row.overall)).cache()


# In[11]:

intmax = 2 ** 31 - 1
rawRatings3 = ratings.map(lambda row: (abs(hash(row.reviewerID))%intmax, abs(hash(row.asin))%intmax, row.overall)).cache()


# In[12]:

from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating


# In[13]:

rank = 10
numIterations = 10
model = ALS.train(rawRatings3, rank, numIterations)


# In[ ]:

#model.recommendProducts(8751,5)


# In[95]:

#model.userFeatures().take(5)


# In[57]:

#userMoviePair = usersDf.join(moviesDf).drop('reviewerID').drop('asin')
#userMoviePair.printSchema()
#userMoviePair = userMoviePair.filter(userMoviePair.userID < 1000)
#userMoviePair = userMoviePair.map(lambda row: (row.userID, row.movieID)).cache()


# In[ ]:




# In[73]:

moviePool = sqlContext.read.json("s3n://patricks3db/meta_Movies_and_TV.json")
#moviePool.printSchema()
print moviePool.count()
moviePool = moviePool.filter(moviePool.imUrl.endswith('jpg') | moviePool.imUrl.endswith('jpeg'))
print moviePool.count()
intmax = 2 ** 31 - 1
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
createIntegerId = udf(lambda text: abs(hash(text))%intmax, IntegerType())
moviePool = moviePool.withColumn('pid', createIntegerId(moviePool.asin))
#moviePool.printSchema()
moviePool = moviePool.select('pid', 'asin', 'brand', 'imUrl', 'price', 'title')
moviePool.first()


# In[ ]:




# In[16]:

def AddToCassandra_allcountsbatch_bypartition(d_iter):
        class movieCatalog2(Model):
                from cqlengine import columns
                from cqlengine.models import Model
                from cqlengine import connection
                from cqlengine.management import sync_table
                CASSANDRA_KEYSPACE = "playground"
                pid = columns.Integer(primary_key=True)
                asin = columns.Text(primary_key=True)
                brand = columns.Text()
                imUrl = columns.Text()
                price = columns.Float()
                title = columns.Text()
        connection.setup(['172.31.39.226'], CASSANDRA_KEYSPACE)
        sync_table(movieCatalog2)
        for d in d_iter:
                movieCatalog2.create(**d)
# Create table if it does not exist. Need to do this before submitting to Spark to avoid collisions
AddToCassandra_allcountsbatch_bypartition([])


# In[17]:

moviePool.toJSON(use_unicode=False).map(lambda row: json.loads(row)).foreachPartition(AddToCassandra_allcountsbatch_bypartition)


# In[18]:

userPool = df.select('reviewerID', 'reviewerName').distinct()
intmax = 2 ** 31 - 1
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
createIntegerId = udf(lambda text: abs(hash(text))%intmax, IntegerType())
userPool = userPool.withColumn('uid', createIntegerId(userPool.reviewerID))
userPool.printSchema()


# In[19]:

def AddToCassandra_allcountsbatch_bypartition(d_iter):
        class userbase2(Model):
                from cqlengine import columns
                from cqlengine.models import Model
                from cqlengine import connection
                from cqlengine.management import sync_table
                CASSANDRA_KEYSPACE = "playground"
                uid = columns.Integer(primary_key=True)
                reviewerID = columns.Text(primary_key=True)
                reviewerName = columns.Text()
        connection.setup(['172.31.39.226'], CASSANDRA_KEYSPACE)
        sync_table(userbase2)
        for d in d_iter:
                userbase2.create(**d)
# Create table if it does not exist. Need to do this before submitting to Spark to avoid collisions
AddToCassandra_allcountsbatch_bypartition([])


# In[20]:

userPool.toJSON(use_unicode=False).map(lambda row: json.loads(row)).foreachPartition(AddToCassandra_allcountsbatch_bypartition)


# In[21]:

userPool.count()


# In[58]:

userMoviePair = userPool.select('uid').join(moviePool.select('pid')).cache()
userMoviePair.printSchema()
userMoviePair.rdd.getNumPartitions()


# In[ ]:

recommendations = model.predictAll(userMoviePair.rdd.cache())
recommendationsToC = recommendations.map(lambda x: {"user":x[0], "product":x[1], "rating":x[2]}).cache()


# In[90]:

def AddToCassandra_allcountsbatch_bypartition(d_iter):
        from cqlengine import columns
        from cqlengine.models import Model
        from cqlengine import connection
        from cqlengine.management import sync_table
        CASSANDRA_KEYSPACE = "playground"
        class predictions3(Model):
                user = columns.Integer(primary_key=True)
                product = columns.Integer()
                rating = columns.Float(primary_key=True, clustering_order="DESC")
        connection.setup(['172.31.39.226'], CASSANDRA_KEYSPACE)
        sync_table(predictions3)
        for d in d_iter:
                predictions3.create(**d)

# Create table if it does not exist. Need to do this before submitting to Spark to avoid collisions
AddToCassandra_allcountsbatch_bypartition([])
recommendationsToC.foreachPartition(AddToCassandra_allcountsbatch_bypartition)


# In[109]:

"""
#compute recommmendation and save to cassandra
def createRecommendationsForAll(testuid):
    from pyspark.sql.functions import udf
    from pyspark.sql.types import IntegerType
    addUid = udf(lambda inputUid: testuid, IntegerType())
    userMoviePair = moviePool.select('pid')
    #print userMoviePair.printSchema()
    #print type(userMoviePair)
    userMoviePair2 = userMoviePair.withColumn('uid', addUid(userMoviePair.pid)).select('uid','pid')
    #print userMoviePair2.printSchema()
    #print userMoviePair2.take(5)
    recommendations = model.predictAll(userMoviePair2.rdd.cache())
    recommendationsToC = recommendations.map(lambda x: {"user":x[0], "product":x[1], "rating":x[2]}).cache()
"""


# In[110]:

"""
userPool.printSchema()
#userList = userPool.map(lambda row: row.uid).take(50)
userList = userPool.map(lambda row: row.uid).collect()
#print userList
"""


# In[111]:

#for uid in userList: createRecommendationsForAll(uid)


# In[112]:

"""
testuid = 1981581556
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
addUid = udf(lambda inputUid: testuid, IntegerType())
userMoviePair = moviePool.select('pid')
#print userMoviePair.printSchema()
#print type(userMoviePair)
userMoviePair2 = userMoviePair.withColumn('uid', addUid(userMoviePair.pid)).select('uid','pid')
#print userMoviePair2.printSchema()
#print userMoviePair2.take(5)
recommendations = model.predictAll(userMoviePair2.rdd.cache())
print recommendations.take(5)
recommendationsToC = recommendations.map(lambda x: {"user":x[0], "product":x[1], "rating":x[2]}).cache()
print recommendationsToC.take(5)
recommendationsToC.foreachPartition(AddToCassandra_allcountsbatch_bypartition)
"""


# In[ ]:




# In[ ]:




