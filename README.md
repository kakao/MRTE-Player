MRTE-Player
===========

MRTE(MySQL Realtime Traffic Emulator) Player

Architecture
------------
https://github.com/kakao/MRTE-Player/blob/master/doc/mrte.png


How to run
----------
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
--rabbitmq_host="127.0.0.1" \
--rabbitmq_user="guest" \
--rabbitmq_password="" \
--rabbitmq_port=5672 \
--select_only=no 2> error.log
