MRTE-Player
===========

MRTE(MySQL Realtime Traffic Emulator) Player

Architecture
------------
https://github.com/kakao/MRTE-Player/blob/master/doc/mrte.png


How to run
----------
<pre>
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
</pre>


How to run
----------
<pre>
./MRTEPlayer.sh
</pre>


Understanding how to work
-------------------------
MRTEPlayer preparing Rabbit MQ Connections and target MySQL server connections.
If there's no problem, one of child thread will start subscribing message from Rabbit MQ specified by command line options.

Subscribing thread will get the message and split it. After extracting source ip and port from unit-message and parse the mysql packet(payload of tcp/ip packet).
MRTEPlayer only process three types packet. MRTEPlayer make client thread as many as client_ip:port pair of source MySQL server. And message from MRTECollector will go to each client thread based on client_ip:port pair. 

<ul>
<li>UserRequest		: Every sql (query and dml and all types text protocol of mysql client/server communication).</li>
<li>ConnectionOpen	: COM_CONNECT packet. (Actually MRTECollector capture only income server packet, So MRTECollector can't catch COM_CONNECT packet. This is only emulated packet. Source code is good manual, take a look if you want detailed explanation)</li>
<li>ConnectionClose	: COM_QUIT packet.</li>
</ul>


Understanding output
--------------------
MRTEPlayer also print internal processing status every 10 seconds.

<pre>
$ MRTEPlayer.sh
...
DateTime                TotalPacket      ErrorPacket   NewSession   ExitSession      UserRequest        Error (NoInitDB  Duplicated  Deadlock  LockTimeout)
2015-01-02 21:12:20           34153                0            0             0            34154            0 (       0           0         0            0)
2015-01-02 21:12:30           34173                0            0             0            34167            0 (       0           0         0            0)
2015-01-02 21:12:40           34199                0            0             0            34200            0 (       0           0         0            0)
2015-01-02 21:12:50           34192                0            0             0            34192            0 (       0           0         0            0)
2015-01-02 21:13:00           34113                0            0             0            34114            0 (       0           0         0            0)
2015-01-02 21:13:10           34111                0            0             0            34113            0 (       0           0         0            0)
2015-01-02 21:13:20           34138                0            0             0            34134            0 (       0           0         0            0)
2015-01-02 21:13:30           34132                0            0             0            34137            0 (       0           0         0            0)
</pre>
<br>
<ul>
<li>TotalPacket 	: Total count of packet delived via Rabbit MQ</li>
<li>ErrorPacket		: Error count of splitting batched packet</li>
<li>NewSession		: COM_CONNECT packet count</li>
<li>ExitSession		: COM_QUIT packet count</li>
<li>UserRequest		: All sql command packet count</li>
<li>Error			: Total error count occured during processing request to target MySQL server</li>
<li>NoInitDB		: SQL Error (No init database)</li>
<li>Duplicated		: SQL Error (Duplicated error)</li>
<li>Deadlock		: SQL Error (Dead lock)</li>
<li>LockTimeout		: SQL Error (Lock wait timeout exceeded)</li>
</ul>


Limitations
-----------
<ol>
<li>MRTEPlayer can't guarantee the order of transactions of source MySQL server.<br>
   I tried a lot of efforts to keep the original transactions order to target MySQL server like transaction split by hashed queue and transfer based on the order of source MySQL server.<br>
   This will reserve the order of transaction but not 100%.<br>  
   So we count and print some SQL error count like Deadlock and Lock wait-timeout.<br>
</ol>