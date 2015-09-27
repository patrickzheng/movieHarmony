#!/bin/sh
. ~/.profile
. ~/globalVars.sh

sudo echo "$NamenodeDSN" "$NamenodeIP" >> /etc/hosts
sudo echo "$Datanode1DSN" "$Datanode1IP" >> /etc/hosts
sudo echo "$Datanode2DSN" "$Datanode2IP" >> /etc/hosts
sudo echo "$Datanode3DSN" "$Datanode3IP" >> /etc/hosts
sudo echo "$Datanode4DSN" "$Datanode4IP" >> /etc/hosts

sudo sed -i.bak 's|<configuration>|&<property> <name>dfs.replication</name> <value>3</value> </property> <property> <name>dfs.namenode.name.dir</name> <value>file:///usr/local/hadoop/hadoop_data/hdfs/namenode</value> </property>|' $HADOOP_HOME/etc/hadoop/hdfs-site.xml

sudo mkdir -p $HADOOP_HOME/hadoop_data/hdfs/namenode
sudo chown -R ubuntu $HADOOP_HOME

sudo echo "$NamenodeIP" >>$HADOOP_HOME/etc/hadoop/masters

sudo sed -i.bak 's|localhost||' $HADOOP_HOME/etc/hadoop/slaves

sudo echo "$Datanode1IP" >>$HADOOP_HOME/etc/hadoop/slaves
sudo echo "$Datanode2IP" >>$HADOOP_HOME/etc/hadoop/slaves
sudo echo "$Datanode3IP" >>$HADOOP_HOME/etc/hadoop/slaves
sudo echo "$Datanode4IP" >>$HADOOP_HOME/etc/hadoop/slaves
