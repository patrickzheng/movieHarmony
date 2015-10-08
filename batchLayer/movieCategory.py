
# coding: utf-8

# In[1]:

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
df = sqlContext.read.json("s3n://patricks3db/meta_Movies_and_TV.json")
df.printSchema()


# In[2]:

df.select("categories").take(100)


# In[ ]:



