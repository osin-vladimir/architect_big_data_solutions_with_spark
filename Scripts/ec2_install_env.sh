sudo apt-get update -y
sudo apt-get upgrade -y
sudo add-apt-repository -y ppa:webupd8team/java
sudo apt-get update -y
sudo apt-get install oracle-java8-installer -y
sudo apt-get install zookeeperd -y
wget http://apache.claz.org/kafka/1.0.0/kafka_2.11-1.0.0.tgz 
sudo mkdir /opt/Kafka
sudo tar -xvf ./kafka_2.11-1.0.0.tgz -C /opt/Kafka/
sudo apt-get install scala -y
sudo apt-get install git -y
wget http://apache.claz.org/spark/spark-2.2.1/spark-2.2.1-bin-hadoop2.7.tgz

sudo apt-get install screen -y

tar xzvf spark-2.2.1-bin-hadoop2.7.tgz
mv spark-2.2.1-bin-hadoop2.7/ spark
sudo mv spark/ /usr/lib/

echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
sudo apt-get update -y
sudo apt-get install sbt -y

cp /usr/lib/spark/conf/spark-env.sh.template /usr/lib/spark/conf/spark-env.sh

echo 'JAVA_HOME=/usr/lib/jvm/java-8-oracle' >> /usr/lib/spark/conf/spark-env.sh
echo 'SPARK_WORKER_MEMORY=4g' >> /usr/lib/spark/conf/spark-env.sh

echo 'net.ipv6.conf.all.disable_ipv6 = 1' >> /etc/sysctl.conf
echo 'net.ipv6.conf.default.disable_ipv6 = 1' >> /etc/sysctl.conf
echo 'net.ipv6.conf.lo.disable_ipv6 = 1' >> /etc/sysctl.conf

echo 'export JAVA_HOME=/usr/lib/jvm/java-8-oracle' >> ~/.bashrc
echo 'export SBT_HOME=/usr/share/sbt-launcher-packaging/bin/sbt-launch.jar' >> ~/.bashrc
echo 'export SPARK_HOME=/usr/lib/spark' >> ~/.bashrc
echo 'export PATH=$PATH:$JAVA_HOME/bin' >> ~/.bashrc
echo 'export PATH=$PATH:$SBT_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin' >> ~/.bashrc
echo 'export PYSPARK_PYTHON=python3' >> ~/.bashrc

sudo apt install python3-pip -y
sudo pip3 install pandas
sudo pip3 install pykafka
mkdir /root/spark_checkpoint/
