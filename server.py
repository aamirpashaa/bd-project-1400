import redis
from flask import Flask, url_for
import datetime, json

r = redis.Redis(host='localhost', port=6379, db=1, decode_responses=True)
app = Flask(__name__)


@app.route('/redis/channels/<time_range>/')
def channels_posts(time_range):
	now = datetime.datetime.now()
	now = [now - datetime.timedelta(hours=x) for x in range(0, int(time_range))]
	keys = ['post:[\u0621-\u0628\u062A-\u063A\u0641-\u0642\u0644-\u0648\u064E-\u0651\u0655\u067E\u0686\u0698\u06A9\u06AF\u06BE\u06CC]*#{}'.format(x.strftime("%Y-%m-%dT%H")) for x in now]
	result = {}
	
	for key_pattern in keys:
		for key in r.keys(key_pattern):
			try:
				k = key.split(':')[1]
				k = k.split('#')[0]
				result[k] = result.get(k, 0) + int(r.get(key))
			except:
				continue
	return json.dumps({"total_number_of_posts": result})



@app.route('/redis/hashtags/<time_range>/')
def hashtags(time_range):
	now = datetime.datetime.now()
	now = [now - datetime.timedelta(hours=x) for x in range(0, int(time_range))]
	keys = ['hashtag:[\u0621-\u0628\u062A-\u063A\u0641-\u0642\u0644-\u0648\u064E-\u0651\u0655\u067E\u0686\u0698\u06A9\u06AF\u06BE\u06CC]*#{}'.format(x.strftime("%Y-%m-%dT%H")) for x in now]
	result = {}
	
	for key_pattern in keys:
		for key in r.keys(key_pattern):
			try:
				k = key.split(':')[1]
				k = k.split('#')[0]
				result[k] = result.get(k, 0) + int(r.get(key))
			except:
				continue
	return json.dumps({"total_number_of_hashtags": result})



@app.route('/redis/total_posts/<time_range>/')
def total_posts(time_range):
	now = datetime.datetime.now()
	now = [now - datetime.timedelta(hours=x) for x in range(0, int(time_range))]
	keys = ['post:[\u0621-\u0628\u062A-\u063A\u0641-\u0642\u0644-\u0648\u064E-\u0651\u0655\u067E\u0686\u0698\u06A9\u06AF\u06BE\u06CC]*#{}'.format(x.strftime("%Y-%m-%dT%H")) for x in now]
	
	result = 0
	for key_pattern in keys:
		for key in r.keys(key_pattern):
			try:
				result = result + int( r.get(key) )
			except:
				continue
	return json.dumps({"total_number_of_posts": result})



@app.route('/redis/recent_posts/')
def recent_posts():
	result = r.lrange('recent_posts', 0, -1)
	return json.dumps({"recent_posts": result})



@app.route('/redis/recent_hashtags/')
def recent_hashtags():
	result = r.lrange('recent_hashtags', 0, -1)
	return json.dumps({"recent_hashtags": result})


if __name__ == "__main__":
	app.run(host = 'localhost', port = 8080, debug = True) 