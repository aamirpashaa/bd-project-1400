from kafka import KafkaConsumer, KafkaProducer
import uuid, json, datetime, re
from hazm import *
import yake

def split_date_time(date):
	date = date.split('+')[0]
	timestamp = datetime.datetime.timestamp(datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S"))
	date, time = date.split(' ')

	return date, time, timestamp

def clean_message(message):
	normalizer = Normalizer()
	stop_words = []
	special_words = []

	# with open("./stopwords.txt", "r") as swf:
	# 	sw = swf.read()
	# 	stop_words = sw.split("\n")
	# swf.close()

	stop_words = [line.rstrip() for line in open("stopwords/persian")]
	stop_words.extend([line.rstrip() for line in open("stopwords/verbal")])
	stop_words.extend([line.rstrip() for line in open("stopwords/nonverbal")])
	stop_words.extend([line.rstrip() for line in open("stopwords/short")])

	message = re.sub(r'http[s]?://\S+', '', message)
	message = re.sub(r'[A-Za-z0-9]+@[a-zA-z].[a-zA-Z]+', '', message)
	message = normalizer.normalize(message)
	message = word_tokenize(message)
	message = [word for word in message if word not in stop_words and word.isalpha()]
	special_words = [word for word in message if word in ['روحانی' , 'بورس', 'اقتصاد', 'دلار', 'یورو', 'تحریم', 'دولت', 'انتخابات', 'طلا', 'کرونا', 'کووید', 'کوید', 'تورم', 'دانشگاه']]
	message = ' '.join(message)

	return message, special_words


def find_hashtags(message):
	hashtags = re.findall(r"#(\w+)", message)
	return list(set(hashtags))

def find_keywords(message):
	message, special_words = clean_message(message)

	kw_extractor = yake.KeywordExtractor()
	custom_kw_extractor = yake.KeywordExtractor(n=1, features=None, top=10)
	keywords = custom_kw_extractor.extract_keywords(message)
	keywords = [x[0] for x in keywords if x[1] > 0.09] + special_words
	return list(set(keywords))


producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10), value_serializer=lambda v: json.dumps(v).encode('utf-8'))
consumer = KafkaConsumer('preprocess', auto_offset_reset='earliest', bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000, value_deserializer=lambda m: json.loads(m.decode('utf-8')))



while(True):
	for item in consumer:
		new_item = item.value
		new_item['UUID'] = str(uuid.uuid1(new_item['id']))

		if 'message' not in new_item:
			continue
		
		new_item['hashtags'] = find_hashtags(new_item['message'])
		new_item['keywords'] = find_keywords(new_item['message'])

		date, time, timestamp = split_date_time(new_item['date'])
		new_item['date'] = date
		new_item['time'] = time
		new_item['timestamp'] = timestamp

		producer.send('persistence', new_item)
		producer.flush()

		print(datetime.datetime.now(),"new message inserted into persistence")
