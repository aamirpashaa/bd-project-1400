from kafka import KafkaConsumer
import json
from cassandra.cluster import Cluster


cluster = Cluster()
session = cluster.connect("telegram")


query = "CREATE TABLE IF NOT EXISTS messages ( date text, time text, post_id text, primary key(date, time, post_id) );"
result = session.execute(query)

query = "CREATE TABLE IF NOT EXISTS channels ( channel_id text, ts float, post_id text, primary key(channel_id, post_id) );"
result = session.execute(query)

query = "CREATE TABLE IF NOT EXISTS keywords ( keyword text, ts float, post_id text, primary key(keyword, post_id) );"
result = session.execute(query)


consumer = KafkaConsumer('persistence', auto_offset_reset='earliest', bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000, value_deserializer=lambda m: json.loads(m.decode('utf-8')))


insert_messages_query = "INSERT INTO messages (date, time, post_id) VALUES (%s, %s, %s)"
insert_channels_query = "INSERT INTO channels (channel_id, ts, post_id) VALUES (%s, %s, %s)"
insert_keywords_query = "INSERT INTO keywords (keyword, ts, post_id) VALUES (%s, %s, %s)"

while(True):
	for item in consumer:
		new_item = item.value
		print('new item recieved')

		session.execute(insert_messages_query, (new_item['date'], new_item['time'], str(new_item['id'])) )
		session.execute(insert_channels_query, (str(new_item['peer_id']['channel_id']), new_item['timestamp'], str(new_item['id'])) )

		#for kw in (new_item['hashtags'] + new_item['keyword']):
		for kw in new_item['hashtags']:
			session.execute(insert_keywords_query, (kw, new_item['timestamp'], str(new_item['id'])) )