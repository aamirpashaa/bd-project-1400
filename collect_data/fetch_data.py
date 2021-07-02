from telegram_client import Client
from telethon import events
from kafka import KafkaProducer
import json, datetime


def date_format(message):
	if isinstance(message, datetime.datetime):
		return message.__str__()
		

new_client = Client()
client = new_client.connect()
channels_list = new_client.get_channels()

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10), value_serializer=lambda v: json.dumps(v, default=date_format).encode('utf-8'))

@client.on(events.NewMessage(chats = channels_list))
async def newMessageListener(event):
	newMessage = event.message
	producer.send('preprocess', newMessage.to_dict())
	producer.flush()


with client: 
	client.run_until_disconnected()