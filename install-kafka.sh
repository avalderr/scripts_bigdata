#/bin/bash!
apt-get update 
yes | apt-get install default-jre
yes | apt-get install zookeeperd
adduser --system --no-create-home --disabled-password --disabled-login kafka
mkdir ~/inst
cd ~/inst
wget https://downloads.apache.org/kafka/2.5.0/kafka_2.12-2.5.0.tgz
mkdir /opt/kafka
tar -xvzf kafka_2.12-2.5.0.tgz --directory /opt/kafka --strip-components 1
cp ~/scripts/confs/server.properties /opt/kafka/config
mkdir /var/lib/kafka
mkdir /var/lib/kafka/data
mkdir /var/lib/kafka
mkdir /var/lib/kafka/data
chown -R kafka:nogroup /opt/kafka
chown -R kafka:nogroup /var/lib/kafka
cp ~/scripts/confs/kafka.service  /etc/systemd/system/
systemctl start kafka.service
systemctl enable kafka.service
