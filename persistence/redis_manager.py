from kafka import KafkaConsumer
import json
import redis


def channels_number_of_posts(name, date):
	ck = 'post:{}#{}'.format(name, date)
	r.incr(ck)


def hashtags_number(hashtags, date):
	for hashtag in hashtags:
		ck = 'hashtag:{}#{}'.format(hashtag, date)
		r.incr(ck)

		r.lpush('recent_hashtags', hashtag)
		r.ltrim('recent_hashtags', 0, 99)

def recent_post(post):
	r.lpush('recent_posts', post)
	r.ltrim('recent_posts', 0, 99)


consumer = KafkaConsumer('persistence', auto_offset_reset='earliest', bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000, value_deserializer=lambda m: json.loads(m.decode('utf-8')))
r = redis.Redis(host='localhost', port=6379, db=1, decode_responses=True)


while(True):
	for item in consumer:
		new_item = item.value

		channels_number_of_posts(new_item['peer_id']['channel_id'], new_item['date'] + ":" + new_item['time']) #new_item['name']
		channels_number_of_posts(new_item['hashtags'], new_item['date'] + ":" + new_item['time'])
		recent_post(new_item['message'])



for elem in r.keys():
	r.delete(elem)