#!/bin/sh

. ~/.profile
. ~/globalVars.sh

sudo apt-get update
sudo apt-get install openjdk-7-jdk
wget http://mirror.symnds.com/software/Apache/hadoop/common/hadoop-2.7.1/hadoop-2.7.1.tar.gz -P ~/Downloads
sudo tar zxvf ~/Downloads/hadoop-*.tar.gz -C /usr/local
sudo mv /usr/local/hadoop-* /usr/local/hadoop

echo 'export JAVA_HOME=/usr' >> ~/.profile
echo 'export PATH=$PATH:$JAVA_HOME/bin' >> ~/.profile
echo 'export HADOOP_HOME=/usr/local/hadoop' >> ~/.profile
echo 'export PATH=$PATH:$HADOOP_HOME/bin' >> ~/.profile

. ~/.profile

sudo sed -i 's/JAVA_HOME=${JAVA_HOME}/JAVA_HOME=\/usr/' $HADOOP_HOME/etc/hadoop/hadoop-env.sh

sudo sed -i.bak 's|<configuration>|&<property><name>fs.defaultFS</name><value>hdfs://'"$NamenodeDSN"':9000</value></property>|' $HADOOP_HOME/etc/hadoop/core-site.xml

sudo sed -i.bak 's|<configuration>|&<property> <name>yarn.nodemanager.aux-services</name> <value>mapreduce_shuffle</value> </property> <property> <name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name> <value>org.apache.hadoop.mapred.ShuffleHandler</value> </property> <property> <name>yarn.resourcemanager.resource-tracker.address</name> <value>'"$NamenodeDSN"':8025</value> </property> <property> <name>yarn.resourcemanager.scheduler.address</name> <value>'"$NamenodeDSN"':8030</value> </property> <property> <name>yarn.resourcemanager.address</name> <value>'"$NamenodeDSN"':8050</value> </property> |' $HADOOP_HOME/etc/hadoop/yarn-site.xml

sudo cp $HADOOP_HOME/etc/hadoop/mapred-site.xml.template $HADOOP_HOME/etc/hadoop/mapred-site.xml

sudo sed -i.bak 's|<configuration>|&<property> <name>mapreduce.jobtracker.address</name> <value>'"$NamenodeDSN"':54311</value> </property> <property> <name>mapreduce.framework.name</name> <value>yarn</value> </property> |' $HADOOP_HOME/etc/hadoop/mapred-site.xml
