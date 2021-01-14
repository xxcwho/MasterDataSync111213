#!/bin/bash

. /etc/profile
PROJECT_DIR=$(dirname $0)/..
cd $PROJECT_DIR
PROJECT_DIR=.
LIB_DIR=$PROJECT_DIR/lib
CONF_DIR=$PROJECT_DIR/conf
LOG_DIR=$PROJECT_DIR/log
JAVA_OPTS=$(cat conf/Application.properties |grep JAVA_OPTS|cut -d= -f2)

if [ -f $PROJECT_DIR/is_running ]
then
        echo "$(date +%F' '%T):Process is running,exit..."
        exit 3
fi
touch $PROJECT_DIR/is_running

for i in $(ls $LIB_DIR); do
	CLASSPATH=$LIB_DIR/$i:$CLASSPATH
done
CLASSPATH=$CLASSPATH:$CONF_DIR

java $JAVA_OPTS -Dfile.encoding=UTF-8 -cp $CLASSPATH -Dlog.dir=$LOG_DIR com.chinacscs.MasterDataSyncApplication ExecuteProcesses $*
if [ $? -ne 0 ];then
    exit -1
fi
rm -f $PROJECT_DIR/is_running