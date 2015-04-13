#!/bin/bash
classname=$1
sourcefile="$classname.java"
jarfile="$classname.jar"

rm -rf out
rm -Rf classes
rm $jarfile
mkdir classes

HADOOP_HOME="/usr/local/hadoop-2.6.0"
HADOOP_COMMON_HOME="${HADOOP_HOME}/share/hadoop/common"
HADOOP_MAPRED_HOME="${HADOOP_HOME}/share/hadoop/mapreduce"
YARN_HOME="${HADOOP_HOME}/share/hadoop/yarn"
JAVA_HOME="${JAVA_HOME}"
echo "Compiling ..."
javac -cp ./joda-time-2.4.jar:./jsoup-1.8.1.jar:$HADOOP_COMMON_HOME/hadoop-common-2.6.0.jar:$HADOOP_MAPRED_HOME/hadoop-mapreduce-client-core-2.6.0.jar:$HADOOP_MAPRED_HOME/lib/log4j-1.2.17.jar:. -d classes $sourcefile
echo "Creating jar ..."
jar -cvf $jarfile -C classes/ .
echo "Executing ..."
java -cp $jarfile:./joda-time-2.4.jar:./jsoup-1.8.1.jar:$HADOOP_COMMON_HOME/hadoop-common-2.6.0.jar:$HADOOP_COMMON_HOME/lib/*:$HADOOP_MAPRED_HOME/hadoop-mapreduce-client-core-2.6.0.jar:$HADOOP_MAPRED_HOME/hadoop-mapreduce-client-jobclient-2.6.0.jar:$HADOOP_MAPRED_HOME/hadoop-mapreduce-client-shuffle-2.6.0.jar:$HADOOP_MAPRED_HOME/hadoop-mapreduce-client-common-2.6.0.jar:$YARN_HOME/*:. $1 $2 $3
