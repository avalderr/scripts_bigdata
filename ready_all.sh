#/bin/bash!
cd /root
./install-kafka.sh
apt install unzip
unzip event_gen/trade_data_01-03-2020.zip -d event_gen/data_crunched/
yes | apt install virtualenv
virtualenv eventgen 
source eventgen/bin/activate
pip install pandas 
echo 'Esta TODO OK, se puede correr event gen'
