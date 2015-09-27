mport kafka

# connect to Kafka cluster
cluster = kafka.KafkaClient("ec2-52-88-192-129.us-west-2.compute.amazonaws.com:9092")
prod = kafka.SimpleProducer(cluster, async = False)

# produce some messages
topic = "meta-topic"
# msg_list = []
# prod.send_messages(topic, *msg_list)

# connect to s3
from boto.s3.connection import S3Connection

from boto.s3.key import Key

conn = S3Connection()
bucket = conn.get_bucket('patricks3db')
key = bucket.get_key("meta_Movies_and_TV.json")

