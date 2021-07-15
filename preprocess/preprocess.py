from kafka import KafkaConsumer, KafkaProducer
import uuid, json, datetime, re
import yake

def split_date_time(date):
	date = date.split('+')[0]
	timestamp = datetime.datetime.timestamp(datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S"))
	date, time = date.split(' ')

	return date, time, timestamp

def find_hashtags(message):
	hashtags = re.findall(r"#(\w+)", message)
	return hashtags

def find_keywords(message):
	kw_extractor = yake.KeywordExtractor()
	max_ngram_size = 1
	deduplication_threshold = 0.9
	numOfKeywords = 5
	custom_kw_extractor = yake.KeywordExtractor(n=max_ngram_size, dedupLim=deduplication_threshold, top=numOfKeywords, features=None)
	keywords = custom_kw_extractor.extract_keywords(message)
	keywords = [x[0] for x in keywords]

	return keywords


producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10), value_serializer=lambda v: json.dumps(v).encode('utf-8'))
consumer = KafkaConsumer('preprocess', auto_offset_reset='earliest', bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000, value_deserializer=lambda m: json.loads(m.decode('utf-8')))



while(True):
	for item in consumer:
		new_item = item.value
		new_item['UUID'] = str(uuid.uuid1(new_item['id']))
		
		new_item['hashtags'] = find_hashtags(new_item['message'])
		new_item['keywords'] = find_keywords(new_item['message'])

		date, time, timestamp = split_date_time(new_item['date'])
		new_item['date'] = date
		new_item['time'] = time
		new_item['timestamp'] = timestamp

		producer.send('persistence', new_item)
		producer.flush()
