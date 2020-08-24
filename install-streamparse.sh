#/bin/bash!
yes | apt install default-jdk

wget https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein
mv lein /bin
chmod +x /bin/lein
lein

wget https://downloads.apache.org/storm/apache-storm-1.2.3/apache-storm-1.2.3.zip
unzip apache-storm-1.2.3.zip
echo 'export PATH="/root/scripts_bigdata/apache-storm-1.2.3/bin/:$PATH"' >> /root/.bashrc
exec bash --login


