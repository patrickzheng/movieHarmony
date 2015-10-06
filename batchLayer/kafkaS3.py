from kafka import KafkaClient, SimpleConsumer
kafka = KafkaClient("ec2-52-26-15-148.us-west-2.compute.amazonaws.com:9092")
consumer = SimpleConsumer(kafka, "my-group", "moviereview9")
while True:
	time.sleep(6)
	messages = consumer.get_messages()
	jsonList = [message.message.value for message in messages]
	
	
