MRTE-Player
===========

MRTE(MySQL Realtime Traffic Emulator) Player

Architecture
------------
https://github.com/kakao/MRTE-Player/blob/master/doc/mrte.png


How to run
----------
You can change MRTEPlayer.sh shell script based on "MRTEPlayer Parameter"

<pre>
./MRTEPlayer.sh
</pre>

MRTEPlayer will print detailed error message standard error stream when error happened. 
So you can append "2> ${MRTE_HOME}/error.log" on MRTEPlayer command line option when you want to collect error message.  


Run parameter
-------------
This is MRTEPlayer option list.

<pre>
<ul>
<li>--mysql_host		: Target MySQL server host name or ip address (e.g. "127.0.0.1")</li>
<li>--mysql_user		: Target MySQL server user account (This user can access #{mysql_default_db})</li>
<li>--mysql_password	: Target MySQL server user password</li>
<li>--mysql_port		: Target MySQL server port (default 3306)</li>
<li>--mysql_init_conn	: How many connection MRTEPlayer should prepare before replaying user request. You may set this value as many as Source MySQL server connection.</li>
<li>--mysql_default_db	: Target MySQL server default database. MRTEPlayer will set this value as all connection's default database</li>
<li>--rabbitmq_host		: Rabbit MQ host name or ip address</li>
<li>--rabbitmq_user		: Rabbit MQ user account (default "guest") </li>
<li>--rabbitmq_password	: Rabbit MQ user password (default "guest")</li>
<li>--rabbitmq_port		: Rabbit MQ server port (default 5672)</li>
<li>--select_only		: Whether MRTEPlayer run only SELECT query or all query (default "no", you can set only "yes" or "now")</li>
</pre>

And you can add JVM option like this. At least you should allow JVM use 1GB of heap.
<pre>
-XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:NewSize=1024M -XX:SurvivorRatio=3 -XX:MaxTenuringThreshold=3 \
-XX:+CMSParallelRemarkEnabled -XX:CMSInitiatingOccupancyFraction=70 -XX:+UseCMSInitiatingOccupancyOnly \
-XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution -XX:+PrintGCApplicationStoppedTime \
-Xloggc:mrte_player_gc.log -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=20M \
-Xmx10G -Xms10G -cp $CLASSPATH com.kakao.mrte.MRTEPlayer \
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
MRTEPlayer also print internal processing status every 10 seconds. And printed value is per second.

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