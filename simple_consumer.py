from kafka import KafkaConsumer
import json

consumer = KafkaConsumer('persistence', auto_offset_reset='earliest', bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000, value_deserializer=lambda m: json.loads(m.decode('utf-8')))

# while(True):
# 	for item in consumer:
# 		print(item.value['message'])
# 		print('\n\n ---------------------------------- \n\n')


while(True):
	for item in consumer:
		for key, value in item.value.items():
			print(key, value)
		print('\n\n ---------------------------------- \n\n')