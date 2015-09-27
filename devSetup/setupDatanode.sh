. ~/.profile

sudo sed -i.bak 's|<configuration>|&<property> <name>dfs.replication</name> <value>3</value> </property> <property> <name>dfs.datanode.data.dir</name> <value>file:///usr/local/hadoop/hadoop_data/hdfs/datanode</value> </property>|' $HADOOP_HOME/etc/hadoop/hdfs-site.xml

sudo mkdir -p $HADOOP_HOME/hadoop_data/hdfs/datanode

sudo chown -R ubuntu $HADOOP_HOME
