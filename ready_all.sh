#/bin/bash!
./install-kafka.sh
apt install unzip
unzip event_gen/trade_data_01-03-2020.zip -d event_gen/crunched_data/
