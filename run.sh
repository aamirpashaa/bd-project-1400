#!/bin/bash

start_services() 
{
	./packages/kafka/bin/zookeeper-server-start.sh ./packages/kafka/config/zookeeper.properties &
	./packages/kafka/bin/kafka-server-start.sh ./packages/kafka/config/server.properties &
}

start_scripts()
{
	. venv/bin/activate
	python3 collect_data/fetch_data.py & 
	python3 preprocess/preprocess.py & 
	python3 persistence/redis_manager.py &
	python3 ./server.py < /dev/null > /dev/null 2>&1 &
}


stop_all() 
{
	killall python3

	./packages/kafka/bin/kafka-server-stop.sh
	./packages/kafka/bin/zookeeper-server-stop.sh
}



case "$1" in
	"start-services")
		start_services
		;;

	"start-scripts")
		start_scripts
		;;

	"stop")
		stop_all
		;;
esac