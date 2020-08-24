#/bin/bash!


apt install make
yes | apt install gcc

wget http://download.redis.io/redis-stable.tar.gz
tar xvzf redis-stable.tar.gz
cd redis-stable
make
cd src
screen -S redis -d -m bash -c './redis-server'
cd ../..

iptables -A INPUT -p tcp --dport  6379  -j ACCEPT
