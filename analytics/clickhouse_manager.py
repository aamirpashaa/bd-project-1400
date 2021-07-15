from kafka import KafkaConsumer
import json, datetime
from clickhouse_driver import Client


consumer = KafkaConsumer('analytics', auto_offset_reset='earliest', bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000, value_deserializer=lambda m: json.loads(m.decode('utf-8')))
client = Client(host='localhost', password='123')


client.execute('CREATE DATABASE IF NOT EXISTS telegram')

create_channels_query = '''
	CREATE TABLE IF NOT EXISTS telegram.channels
		(
			channel_id String,
			sender_name String,
			UUID UUID,
			views Int32,
			timestamp Float32, 
			date DateTime
		) 	
		ENGINE = MergeTree
		PARTITION BY date ORDER BY (timestamp)
'''

create_keywords_query = '''
	CREATE TABLE IF NOT EXISTS telegram.keywords
		(
			keyword String,
			sender_name String,
			timestamp Float32, 
			date DateTime
		) 	
		ENGINE = MergeTree
		PARTITION BY date ORDER BY (timestamp)
'''

client.execute(create_channels_query)
client.execute(create_keywords_query)

while(True):
	for item in consumer:
		new_item = item.value
		
		client.execute(
			'INSERT INTO telegram.channels (channel_id, sender_name, UUID, views, timestamp, date) VALUES',
			[( str(new_item['peer_id']['channel_id']), new_item['sender_name'], new_item['UUID'], new_item['views'], new_item['timestamp'], datetime.datetime.strptime(new_item['date'] + 'T' + new_item['time'], "%Y-%m-%dT%H:%M:%S") )]
		)

		for kw in new_item['hashtags'] + new_item['keywords']:
			client.execute(
				'INSERT INTO telegram.keywords (keyword, sender_name, timestamp, date) VALUES',
				[( kw, new_item['sender_name'], new_item['timestamp'], datetime.datetime.strptime(new_item['date'] + 'T' + new_item['time'], "%Y-%m-%dT%H:%M:%S") )]
			)




#client.execute('DROP TABLE telegram.channels')
# client.execute('DROP TABLE telegram.keywords')
# client.execute('DROP DATABASE telegram')