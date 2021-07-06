#!/bin/bash


trap "exit" INT TERM ERR
trap "kill 0" EXIT

. venv/bin/activate
./packages/kafka/bin/zookeeper-server-start.sh ./packages/kafka/config/zookeeper.properties &
./packages/kafka/bin/kafka-server-start.sh ./packages/kafka/config/server.properties &
python3 collect_data/fetch_data.py & 
python3 preprocess/preprocess.py & 
python3 persistence/redis_manager.py & 
python3 server.py &

wait
