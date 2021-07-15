from kafka import KafkaConsumer, KafkaProducer
import json
from cassandra.cluster import Cluster


producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10), value_serializer=lambda v: json.dumps(v).encode('utf-8'))
consumer = KafkaConsumer('channel_history', auto_offset_reset='earliest', bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000, value_deserializer=lambda m: json.loads(m.decode('utf-8')))


cluster = Cluster()
session = cluster.connect("telegram")

query = "CREATE TABLE IF NOT EXISTS messages ( date text, time text, post_id text, primary key(date, time, post_id) );"
result = session.execute(query)

query = "CREATE TABLE IF NOT EXISTS channels ( channel_id text, ts float, post_id text, primary key(channel_id, ts, post_id)) WITH CLUSTERING ORDER BY (ts DESC, post_id DESC);"
result = session.execute(query)

query = "CREATE TABLE IF NOT EXISTS keywords ( keyword text, ts float, post_id text, primary key(keyword, ts, post_id)) WITH CLUSTERING ORDER BY (ts DESC, post_id DESC);"
result = session.execute(query)


insert_messages_query = "INSERT INTO messages (date, time, post_id) VALUES (%s, %s, %s)"
insert_channels_query = "INSERT INTO channels (channel_id, ts, post_id) VALUES (%s, %s, %s)"
insert_keywords_query = "INSERT INTO keywords (keyword, ts, post_id) VALUES (%s, %s, %s)"

while(True):
	for item in consumer:
		new_item = item.value
		print('new item recieved')

		session.execute(insert_messages_query, (new_item['date'], new_item['time'], new_item['UUID']))
		session.execute(insert_channels_query, (str(new_item['peer_id']['channel_id']), new_item['timestamp'], new_item['UUID']))

		for kw in (new_item['hashtags'] + new_item['keywords']):
			session.execute(insert_keywords_query, (kw, new_item['timestamp'], new_item['UUID']))

		producer.send('statistics', new_item)
		producer.flush()