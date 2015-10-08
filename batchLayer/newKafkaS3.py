
# coding: utf-8

# In[ ]:

import json


# In[1]:

from kafka import KafkaClient, SimpleConsumer
kafka = KafkaClient("ec2-52-26-15-148.us-west-2.compute.amazonaws.com:9092")
consumer = SimpleConsumer(kafka, "my-group", "moviereview9")


# In[10]:

#time.sleep(6)
messages = consumer.get_messages(100)
print messages


# In[41]:

jsonList = [json.loads(message.message.value). for message in messages]
print jsonList


# In[42]:

print json.dumps(jsonList)


# In[43]:

from boto.s3.connection import S3Connection
from boto.s3.key import Key
conn = S3Connection()
conn = S3Connection()
bucket = conn.get_bucket('patricks3db')
#key = bucket.get_key("meta_Movies_and_TV.json")


# In[44]:

k = Key(bucket)


# In[45]:

import time
k.key = '/moviereviews/' + time.strftime("%Y%m%d-%H%M%S") + ".json"
k.set_contents_from_string(json.dumps(jsonList))


# In[ ]:



