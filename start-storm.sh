#/bin/bash!

screen -S nimbus -d -m bash -c 'storm nimbus'
screen -S supervisor -d -m bash -c 'storm supervisor'
screen -S ui -d -m bash -c 'storm ui'

iptables -A INPUT -p tcp --dport 8080 -j ACCEPT

