
# coding: utf-8

# In[1]:

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
#df = sqlContext.read.json("s3n://patricks3db/reviews_Movies_and_TV_small.json")
df = sqlContext.read.json("s3n://patricks3db/meta_Movies_and_TV.json")
df.printSchema()


# In[3]:

mp = df
mp = mp.drop("salesRank")
mp = mp.drop("related")
mp = mp.drop("categories")
mp.columns


# In[4]:

mp.persist(StorageLevel.MEMORY_AND_DISK_SER)


# In[5]:

newMovieDf = sqlContext.read.json("s3n://patricks3db/mapmidasin/midasin.json")


# In[6]:

newMovieDf.take(1)


# In[7]:

newMovieDf.persist(StorageLevel.MEMORY_AND_DISK_SER)


# In[8]:

mp1 = mp.join(newMovieDf, mp.asin == newMovieDf.asin)
mp1.printSchema()


# In[11]:

mp3 = mp1.map(lambda x: {"mid":x.mid, "imurl":x.imUrl, "asin":x.asin, "title":x.title})


# In[12]:

mp3.take(1)


# In[20]:

def syncToCassandra(d_iter):
        from cqlengine import columns
        from cqlengine.models import Model
        from cqlengine import connection
        from cqlengine.management import sync_table
        CASSANDRA_KEYSPACE = "playground"
        class movieprofile9(Model):
                mid = columns.Integer(primary_key=True)
                asin = columns.Text()
                title = columns.Text()
                imurl = columns.Text()
        connection.setup(['172.31.39.226'], CASSANDRA_KEYSPACE)
        sync_table(movieprofile9)
        for d in d_iter:
                movieprofile9.create(**d)
# Create table if it does not exist. Need to do this before submitting to Spark to avoid collisions
syncToCassandra([])
mp3.foreachPartition(syncToCassandra)


# In[ ]:



