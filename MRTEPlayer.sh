#!/bin/bash

CLASSPATH=.:./build/
for jar in ./lib/*.jar; do
    CLASSPATH=$CLASSPATH:$jar
done

## only redirect error log to file (error.log)
java \
-XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:NewSize=1024M -XX:SurvivorRatio=3 -XX:MaxTenuringThreshold=3 \
-XX:+CMSParallelRemarkEnabled -XX:CMSInitiatingOccupancyFraction=70 -XX:+UseCMSInitiatingOccupancyOnly \
-XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution -XX:+PrintGCApplicationStoppedTime \
-Xloggc:mrte_player_gc.log -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=20M \
-Xmx10G -Xms10G -cp $CLASSPATH com.kakao.mrte.MRTEPlayer \
--mysql_host="127.0.0.1" \
--mysql_user="mrte" \
--mysql_password="" \
--mysql_port=3306 \
--mysql_init_conn=200 \
--mysql_default_db="" \
--database_remap="192.168.0.1/db=newdb1,192.168.0.2/db=newdb2,192.168.0.3/db=newdb3,192.168.0.4/db=newdb4" \
--rabbitmq_host="127.0.0.1" \
--rabbitmq_user="guest" \
--rabbitmq_password="" \
--rabbitmq_port=5672 \
--rabbitmq_queue_name="queue1" \
--rabbitmq_routing_key="" \
--select_only=no 2> error.log

## if database_remap option is not present, database mapping is not used internally.