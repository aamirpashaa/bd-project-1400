from kafka import KafkaConsumer, KafkaProducer
import json, datetime
import redis


def channels_number_of_posts(name, date):
	ck = 'post:{}#{}'.format(name, date.split(':')[0])
	r.incr(ck)
	r.expire(ck, datetime.timedelta(days=7))


def keywords_number(keywords, date):
	for kw in keywords:
		ck = 'keyword:{}#{}'.format(kw, date.split(':')[0])
		r.incr(ck)
		r.expire(ck, datetime.timedelta(days=7))

		r.lpush('recent_keywords', kw)
		r.ltrim('recent_keywords', 0, 999)

def recent_posts(post):
	r.lpush('recent_posts', post)
	r.ltrim('recent_posts', 0, 99)


producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10), value_serializer=lambda v: json.dumps(v).encode('utf-8'))
consumer = KafkaConsumer('statistics', auto_offset_reset='earliest', bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000, value_deserializer=lambda m: json.loads(m.decode('utf-8')))

r = redis.Redis(host='localhost', port=6379, db=1, decode_responses=True)


while(True):
	for item in consumer:
		new_item = item.value
		dt = new_item['date'] + "T" + new_item['time']

		channels_number_of_posts(new_item['sender_name'], dt)
		keywords_number(new_item['hashtags'] + new_item['keywords'], dt)
		recent_posts(new_item['message'])

		producer.send('analytics', new_item)
		producer.flush()

		print(datetime.datetime.now(),"new message inserted into analytics")


# for elem in r.keys():
# 	r.delete(elem)