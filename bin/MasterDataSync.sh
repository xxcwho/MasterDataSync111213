#!/bin/bash

. /etc/profile
PROJECT_DIR=$(dirname $0)/..
cd $PROJECT_DIR
PROJECT_DIR=.
LIB_DIR=$PROJECT_DIR/lib
CONF_DIR=$PROJECT_DIR/conf
LOG_DIR=$PROJECT_DIR/log
JAVA_OPTS=$(cat conf/Application.properties |grep JAVA_OPTS|cut -d= -f2)
SYNC_STEP=$(cat conf/Application.properties |grep sync_step|cut -d= -f2)
SYNC_STEP=${SYNC_STEP:-2}

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

if [ $SYNC_STEP -ne 3 ];then
    java $JAVA_OPTS -Dfile.encoding=UTF-8 -cp $CLASSPATH -Dlog.dir=$LOG_DIR com.chinacscs.MasterDataSyncApplication DownloadFromSftp $*
    STATUS=$?
    if [ $STATUS -ne 0 ];then
        rm -f $PROJECT_DIR/is_running
        exit -1
    fi
    if [ $SYNC_STEP -eq 1 ];then
        rm -f $PROJECT_DIR/is_running
        echo "Only download file from sftp!"
        exit 0
    fi
fi

java $JAVA_OPTS -Dfile.encoding=UTF-8 -cp $CLASSPATH -Dlog.dir=$LOG_DIR com.chinacscs.MasterDataSyncApplication StageLoad $*
STATUS=$?
[ $STATUS -eq 255 ] && rm -f $PROJECT_DIR/is_running
if [ $STATUS -ne 0 ];then
    exit -1
fi
if [ -n "$(cat conf/Application.properties|grep '^target.jdbc.driver')" ]; then
    java $JAVA_OPTS -Dfile.encoding=UTF-8 -cp $CLASSPATH -Dlog.dir=$LOG_DIR com.chinacscs.MasterDataSyncApplication TargetLoad $*
    STATUS=$?
    [ $STATUS -eq 255 ] && rm -f $PROJECT_DIR/is_running
    if [ $STATUS -ne 0 ];then
        exit -1
    fi
fi
java $JAVA_OPTS -Dfile.encoding=UTF-8 -cp $CLASSPATH -Dlog.dir=$LOG_DIR com.chinacscs.MasterDataSyncApplication ExecuteProcesses $*
STATUS=$?
[ $STATUS -eq 255 ] && rm -f $PROJECT_DIR/is_running
if [ $STATUS -ne 0 ];then
    exit -1
fi
rm -f $PROJECT_DIR/is_running
echo "All processes succeeded!"
