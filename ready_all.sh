#/bin/bash!
./make_swap.sh
./install-kafka.sh
./get-data.sh
./install-streamparse.sh
./install-redis.sh
./start-storm.sh
echo 'Esta TODO OK, se puede correr event gen'

