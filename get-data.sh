#bin/bash!
apt install unzip
unzip event_gen/trade_data_01-03-2020.zip -d event_gen/data_crunched/
yes | apt install virtualenv
virtualenv env 
source env/bin/activate
pip install pandas 
pip install kafka-python
