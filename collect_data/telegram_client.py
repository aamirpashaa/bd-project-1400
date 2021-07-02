from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetDialogsRequest
from telethon.tl.types import InputPeerEmpty, PeerChannel
from telethon.tl.functions.channels import JoinChannelRequest


class Client:
	api_id = 6502227
	api_hash = 'a70e206c9d4e0160a21d058488c5524c'
	phone = '+989372409384'

	client = TelegramClient(phone, api_id, api_hash)

	@classmethod
	def connect(cls):
		cls.client.connect()
		if not cls.client.is_user_authorized():
			cls.client.send_code_request(cls.phone)
			cls.client.sign_in(cls.phone, input('Enter the code: '))

		return cls.client


	@classmethod
	def get_channels(cls):
		req = GetDialogsRequest(offset_date=None, offset_id=0, offset_peer=InputPeerEmpty(), limit=100, hash = 0)
		channels_list = cls.client(req)
		channels_list = [PeerChannel(chat.id) for chat in channels_list.chats]

		return channels_list


	@staticmethod
	def join_channels():
		with open('channels.txt') as f: 
			channels = f.read().splitlines()

		Client.connect()
		for channel in channels:
			try:
				Client.client(JoinChannelRequest(channel))
			except:
				continue