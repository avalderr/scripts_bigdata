#/bin/bash!

screen -S nimbus -d -m bash -c 'storm nimbus'
screen -S supervisor -d -m bash -c 'storm supervisor'
screen -S ui -d -m bash -c 'storm ui'

iptables -A INPUT -p tcp --dport 8080 -j ACCEPT


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
