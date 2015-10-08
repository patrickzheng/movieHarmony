from pyspark import SparkContext
sc = SparkContext(appName="TestApp")

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

# text_file = sc.textFile("s3://amazon-reviews/complete.json")
# text_file = sc.textFile(    "s3://patricks3db/meta_Movies_and_TV.json")
# df = sqlContext.read.json("s3://patricks3db/meta_Movies_and_TV.json")

# connect to cassandra
from cassandra.cluster import Cluster
# cluster = Cluster(['172.31.39.223', '172.31.39.224','172.31.39.225','172.31.39.226'])
cluster = Cluster(['172.31.39.226'])
session = cluster.connect()

insert_statment = session.prepare('INSERT INTO playground.metadata JSON ? ;')
# counts = text_file.map(lambda line: session.execute(insert_statment, line))

import json
# test another way
from boto.s3.connection import S3Connection
from boto.s3.key import Key
conn = S3Connection()
conn = S3Connection()
bucket = conn.get_bucket('patricks3db')
key = bucket.get_key("meta_Movies_and_TV.json")
for i,line in enumerate(key.get_contents_as_string().splitlines()[:]):
        line = line.replace("'","''")
        jsonObject = json.loads(line)
        if "title" in jsonObject.keys():
                jsonObject["title"] = jsonObject["title"]
        if "related" in jsonObject.keys():
                for key in jsonObject["related"].keys():
                        jsonObject["related"][key] = '|'.join(jsonObject["related"][key])
        if "categories" in jsonObject.keys():
                jsonObject["categories"] = ['|'.join(c) for c in jsonObject["categories"]]
        print i
        session.execute("insert into playground.metadata json '" + json.dumps(jsonObject) + "';")

key = bucket.get_key("reviews_Movies_and_TV.json")
for i,line in enumerate(key.get_contents_as_string().splitlines()[:]):
        line = line.replace("'","''")
        jsonObject = json.loads(line)
        session.execute("insert into playground.review json '" + json.dumps(jsonObject) + "';")

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
df = sqlContext.read.json("s3n://patricks3db/reviews_Movies_and_TV.json")
# df = sqlContext.read.json("s3n://patricks3db/meta_Movies_and_TV.json")
# text_file = sc.textFile("s3n://patricks3db/reviews_Movies_and_TV.json")
# text_file = sc.textFile("s3n://patricks3db/meta_Movies_and_TV.json")
# text_file.map(lambda x: (x,1))
# reviewerSummary = df.map(lambda row: (row.reviewerID,[(row.asin, row.overall)])).collect()
# reviewerSummaryRDD = df.map(lambda row: (row.reviewerID,list([(row.asin, row.overall), ]))).groupByKey()
# reviewerSummary = reviewerSummaryRDD.map(lambda x: {"reviewerID":x[0], "reviewDetail":[tup for sublist in x[1] for tup in sublist]}).collect()
# reviewerSummary = reviewerSummaryRDD.map(lambda x: {"reviewerID":x[0], "reviewDetail":{tup[0]:tup[1] for sublist in x[1] for tup in sublist}}).collect()
reviewerSummaryRDD1 = df.map(lambda row: (row.reviewerID, {(row.asin, row.overall), }))
reviewerSummaryRDD2 = reviewerSummaryRDD1.reduceByKey(lambda x, y: x | y)
reviewerSummaryRDD3 = reviewerSummaryRDD2.map(lambda x: {"reviewerID":x[0], "reviews":{item[0]:item[1] for item in x[1]}})
def AddToCassandra_allcountsbatch_bypartition(d_iter):
	from cqlengine import columns
	from cqlengine.models import Model
	from cqlengine import connection
	from cqlengine.management import sync_table
	CASSANDRA_KEYSPACE = "playground"
	class reviewerProfile(Model):
		reviewerID = columns.Text(primary_key=True)
		reviews = columns.Map(columns.Text, columns.Float)
	connection.setup(['172.31.39.226'], CASSANDRA_KEYSPACE)
	sync_table(reviewerProfile)
	for d in d_iter:
        	reviewerProfile.create(**d)

# Create table if it does not exist. Need to do this before submitting to Spark to avoid collisions
AddToCassandra_allcountsbatch_bypartition([])
reviewerSummaryRDD3.foreachPartition(AddToCassandra_allcountsbatch_bypartition)
