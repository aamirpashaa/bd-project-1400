from telegram_client import Client
from telethon import functions, events
from kafka import KafkaProducer
import json, datetime
import pytz

def date_format(message):
	if isinstance(message, datetime.datetime):
		return message.__str__()
		

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10), value_serializer=lambda v: json.dumps(v, default=date_format).encode('utf-8'))


def get_messages_at_date(ch, days, end_date):
	for chat in ch:
		for new_message in client.iter_messages(chat, offset_date=end_date):
			if (end_date - (new_message.date).replace(tzinfo=None) ).days > days:
				break

			channel = client.get_entity(new_message.peer_id.channel_id)
			new_message = new_message.to_dict()
			new_message['sender_name'] = channel.title
			
			producer.send('preprocess', new_message)
			producer.flush()

			print(datetime.datetime.now(),"new message inserted into preprocess")

new_client = Client()
client = new_client.connect()
channels_list = new_client.get_channels()

days = 10
end_time = datetime.datetime.now()
get_messages_at_date(ch = channels_list, days=days, end_date=end_time)
