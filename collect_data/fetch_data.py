from telegram_client import Client
from telethon import functions, events
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
	new_message = event.message

	channel = await client.get_entity(new_message.peer_id.channel_id)
	new_message = new_message.to_dict()
	new_message['name'] = channel.title

	producer.send('preprocess', new_message)
	producer.flush()


with client:
	client.run_until_disconnected()