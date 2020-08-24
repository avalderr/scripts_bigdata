#/bin/bash!
./make_swap.sh
./install-kafka.sh
./get-data.sh
./install-storm.sh
./install-redis.sh
./start-storm.sh
echo 'Esta TODO OK, se puede correr event gen'

exec bash --login

