package com.kakao.mrte;

import java.sql.Connection;
import java.sql.DriverManager;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.kakao.util.ByteHelper;
import com.kakao.util.CommandLineOption;
import com.kakao.util.DatabaseMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;

public class MRTEPlayer {
	public static boolean IS_DEBUG = false;

	private final int STATUS_INTERVAL_SECOND = 10;
	
	private SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");	
	
	protected AtomicBoolean shutdown = new AtomicBoolean(false);

	public long recvPacketCounter = 0;
	public long errorPacketCounter = 0;
	public AtomicLong playerErrorCounter = new AtomicLong(0);
	public AtomicLong newSessionCounter = new AtomicLong(0);
	public AtomicLong closeSessionCounter = new AtomicLong(0);
	public AtomicLong userRequestCounter = new AtomicLong(0);
	public AtomicLong duplicateKeyCounter = new AtomicLong(0);
	public AtomicLong deadlockCounter = new AtomicLong(0);
	public AtomicLong lockTimeoutCounter = new AtomicLong(0);
	public AtomicLong noInitDatabsaeCounter = new AtomicLong(0);
	public AtomicLong longQueryCounter = new AtomicLong(0);
	
	int initMysqlConnCount = 300;
	String mysqlHost;
	int mysqlPort;
	String mysqlUser;
	String mysqlPassword;
	String jdbcUrl;
	
	DatabaseMapper dbMapper = null;
	
	/**
	 * if defaultDatabase==null then, MRTEPlayer will guess default db from query statement,
	 * else MRTEPlayer will not guess default db and just use it.
	 */
	String defaultDatabase;
	long connectionPreparedTs;
	ConcurrentLinkedQueue<Connection> preparedConnQueue;
	
	boolean replaySelectOnly;
	long slowQueryTime = 1000 /* Milli-seconds */;
	
	String mqHost;
	String mqUser;
	String mqPassword;
	int mqPort;
	String mqUrl;
	String mqQueueName;
	String mqRoutingKey;
	
	Map<String, SQLPlayer> playerMap = new HashMap<String, SQLPlayer>();
	
	public static void main(String[] args) throws Exception{
		MRTEPlayer player = new MRTEPlayer();
		
		CommandLineOption options = new CommandLineOption(args);
		
		try{
			player.mysqlHost = options.getStringParameter("mysql_host");
			player.mysqlUser = options.getStringParameter("mysql_user");
			player.mysqlPassword = options.getStringParameter("mysql_password", "");
			player.mysqlPort = options.getIntParameter("mysql_port", 3306);
			player.initMysqlConnCount = options.getIntParameter("mysql_init_conn", 300);
			player.defaultDatabase = options.getStringParameter("mysql_default_db", null);
			if(player.defaultDatabase==null || player.defaultDatabase.trim().length()<=0)
				player.defaultDatabase = null;
			
			player.slowQueryTime = options.getLongParameter("slow_query_time", 1000);
			
			player.mqHost = options.getStringParameter("rabbitmq_host");
			player.mqUser = options.getStringParameter("rabbitmq_user");
			player.mqPassword = options.getStringParameter("rabbitmq_password", "");
			player.mqPort = options.getIntParameter("rabbitmq_port", 5672);
			
			player.replaySelectOnly = options.getBooleanParameter("select_only", false);
			String dbMapOption = options.getStringParameter("database_remap", null);
			if(dbMapOption!=null && dbMapOption.trim().length()>0){
				player.dbMapper = new DatabaseMapper();
				player.dbMapper.parseDatabaseMapping(options.getStringParameter("database_remap"));
			}
			player.mqQueueName = options.getStringParameter("rabbitmq_queue_name", "");
			player.mqRoutingKey = options.getStringParameter("rabbitmq_routing_key", "");
		}catch(Exception ex){
			ex.printStackTrace();
			options.printHelp(80, 5, 3, true, System.out);
			return;
		}
		
		if(player.defaultDatabase==null){
			player.jdbcUrl = "jdbc:mysql://"+player.mysqlHost+":"+player.mysqlPort+"/?autoReconnect=true";
		}else{
			player.jdbcUrl = "jdbc:mysql://"+player.mysqlHost+":"+player.mysqlPort+"/"+player.defaultDatabase+"?autoReconnect=true";
		}
		player.mqUrl = "amqp://"+player.mqUser+":"+player.mqPassword+"@"+player.mqHost+":"+player.mqPort+"/";
		
		player.runMRTEPlayer();
	}
	
	protected void runMRTEPlayer() throws Exception{
		// prepare target mysql connection
		this.prepareConnections();
		
		// init rabbitmq consumer
		ConnectionFactory factory = new ConnectionFactory();
	    factory.setHost(mqHost);
	    factory.setUsername(mqUser);
	    factory.setPassword(mqPassword);
	    factory.setPort(mqPort);
	    com.rabbitmq.client.Connection connection = factory.newConnection();

	    // This procedure will be handled by user manually
	    // Channel channel = connection.createChannel();
	    // channel.queueDeclare(mqQueueName, false, false, false, null);
	    // channel.queueBind(mqQueueName, EXCHANGE_NAME, ROUTING_KEY);
	    // channel.close();
	    // System.err.println("    >> MRTEPlayer :: Declare and binding queue with queue-name : " + mqQueueName);
	    
	    // Create Rabbit MQ consumer and start it
	    MQueueConsumer consumer = new MQueueConsumer(this, connection.createChannel(), mqQueueName, mqRoutingKey/*, ExecutorService threadExecutor*/);

	    // Print status
	    long pRecvPacketCounter = 0;
	    long pErrorPacketCounter = 0;
	    long pNewSessionCounter = 0;
	    long pCloseSessionCounter = 0;
	    long pUserRequestCounter = 0;
	    long pPlayerErrorCounter = 0;
	    long pDuplicateKeyCounter = 0;
	    long pDeadlockCounter = 0;
	    long pLockTimeoutCounter = 0;
	    long pNoInitDatabsaeCounter = 0;
	    long pLongQueryCounter = 0;

	    long cRecvPacketCounter = 0;
	    long cErrorPacketCounter = 0;
	    long cNewSessionCounter = 0;
	    long cCloseSessionCounter = 0;
	    long cUserRequestCounter = 0;
	    long cPlayerErrorCounter = 0;
	    long cDuplicateKeyCounter = 0;
	    long cDeadlockCounter = 0;
	    long cLockTimeoutCounter = 0;
	    long cNoInitDatabsaeCounter = 0;
	    long cLongQueryCounter = 0;
	    
	    int loopCounter = 0;
	    while(true){
	    	if(loopCounter%20==0){
	    		System.out.println();
	    		System.out.println("DateTime                TotalPacket      ErrorPacket   NewSession   ExitSession      UserRequest(Slow)        Error (NoInitDB  Duplicated  Deadlock  LockTimeout)");
	    		loopCounter = 0;
	    	}
	    	
	    	cRecvPacketCounter = recvPacketCounter;
	    	cErrorPacketCounter = errorPacketCounter;
	    	cNewSessionCounter = newSessionCounter.get();
	    	cCloseSessionCounter = closeSessionCounter.get();
	    	cUserRequestCounter = userRequestCounter.get();
	    	cPlayerErrorCounter = playerErrorCounter.get();
	    	cDuplicateKeyCounter = duplicateKeyCounter.get();
	    	cDeadlockCounter = deadlockCounter.get();
	    	cLockTimeoutCounter = lockTimeoutCounter.get();
	    	cNoInitDatabsaeCounter = noInitDatabsaeCounter.get();
	    	cLongQueryCounter = longQueryCounter.get();
	    	
	    	System.out.format("%s %15d  %15d   %10d    %10d  %15d(%4d)  %11d (%8d  %10d  %8d  %11d)\n", dateFormatter.format(new Date()), 
	    			(long)((cRecvPacketCounter - pRecvPacketCounter) / STATUS_INTERVAL_SECOND),
	    			(long)((cErrorPacketCounter - pErrorPacketCounter) / STATUS_INTERVAL_SECOND),
	    			(cNewSessionCounter - pNewSessionCounter),
	    			(cCloseSessionCounter - pCloseSessionCounter),
	    			(long)((cUserRequestCounter - pUserRequestCounter) / STATUS_INTERVAL_SECOND),
	    			(long)((cLongQueryCounter - pLongQueryCounter) / STATUS_INTERVAL_SECOND),
	    			(long)((cPlayerErrorCounter - pPlayerErrorCounter) / STATUS_INTERVAL_SECOND),
	    			(long)((cNoInitDatabsaeCounter - pNoInitDatabsaeCounter) / STATUS_INTERVAL_SECOND),
	    			(long)((cDuplicateKeyCounter - pDuplicateKeyCounter) / STATUS_INTERVAL_SECOND),
	    			(long)((cDeadlockCounter - pDeadlockCounter) / STATUS_INTERVAL_SECOND),
	    			(long)((cLockTimeoutCounter - pLockTimeoutCounter) / STATUS_INTERVAL_SECOND));
	    	
	    	if(cRecvPacketCounter==pRecvPacketCounter && cErrorPacketCounter==pErrorPacketCounter){
	    		// There's no activity on MQueueConsumer
	    		// I am not sure what happen to Rabbit MQ. Just re open rabbit mq connection & consumer
	    		try{
	    			consumer.close();
	    			connection.close();
	    		}catch(Exception ignore){
	    			System.err.println("    >> MRTEPlayer :: Could not close rabbit mq connection");
	    			ignore.printStackTrace(System.err);
	    		}
	    		
	    		connection = factory.newConnection();
	    		consumer = new MQueueConsumer(this, connection.createChannel(), mqQueueName, mqRoutingKey/*, ExecutorService threadExecutor*/);
	    	}
	    	
	    	loopCounter++;
	    	pRecvPacketCounter = cRecvPacketCounter;
	    	pErrorPacketCounter = cErrorPacketCounter;
	    	pNewSessionCounter = cNewSessionCounter;
	    	pCloseSessionCounter = cCloseSessionCounter;
	    	pUserRequestCounter = cUserRequestCounter;
	    	pPlayerErrorCounter = cPlayerErrorCounter;
	    	pDuplicateKeyCounter = cDuplicateKeyCounter;
	    	pDeadlockCounter = cDeadlockCounter;
	    	pLockTimeoutCounter = cLockTimeoutCounter;
	    	pNoInitDatabsaeCounter = cNoInitDatabsaeCounter;
	    	pLongQueryCounter = cLongQueryCounter;
	    	
	    	try{
	    		Thread.sleep(STATUS_INTERVAL_SECOND * 1000);
	    	}catch(Exception ignore){}
	    }
	}
	
	
	
	
	
	
	
	
	
	protected void destroyPreparedConnections(){
		if( (System.currentTimeMillis() - this.connectionPreparedTs > 60*10*1000 /* 10 min */) && this.preparedConnQueue.size()>0){
			Connection conn1;
			while( (conn1 = this.preparedConnQueue.poll()) != null){
				try{
					conn1.close();
				}catch(Exception ignore){}
			}
		}
	}
	
	protected ConcurrentLinkedQueue<Connection> prepareConnections() throws Exception{
		ConcurrentLinkedQueue<Connection> connQueue = new ConcurrentLinkedQueue<Connection>();
		System.out.println("    >> MRTEPlayer :: Preparing target database connection");
		for(int idx=1; idx<=initMysqlConnCount; idx++){
			Connection conn = DriverManager.getConnection(jdbcUrl, mysqlUser, mysqlPassword);
			connQueue.add(conn); // <--> poll(), poll will return null if no more item on queue
			System.out.print(".");
			if(idx%100 == 0){
				System.out.println(" --> Prepared " + idx + " connections");
			}
			try{
				Thread.sleep(50); // Give a sleep time for stable connection preparing
			}catch(Exception ignore){}
		}
		
		System.out.println("    >> MRTEPlayer :: Done preparing "+initMysqlConnCount+" target database connection");
		this.preparedConnQueue = connQueue;
		this.connectionPreparedTs = System.currentTimeMillis(); // These connections will be closed when 10 minites ago from now.
		return connQueue;
	}
	
	protected String generateSessionKey(String ipAddress, int port){
		return ipAddress + ":" + String.valueOf(port);
	}
	
	protected void stopAllPlayers(){
		this.shutdown.set(true);
	}
	
	
	
	
	
	
	
	
	
	
	// ---------------------------------------------------------------------------------------
	// Packet handling
	// ---------------------------------------------------------------------------------------
	protected void processCloseSession(String sourceServerIp, byte[][] parts) throws Exception{
		String sourceIp = ByteHelper.readIpString(parts[0], 0);
		int sourcePort = ByteHelper.readUnsignedShortLittleEndian(parts[1], 0);
		String sessionKey = generateSessionKey(sourceIp, sourcePort);
		
		if(IS_DEBUG)
			System.out.println("   - [Packet] Close session : ["+sessionKey+"]");
		
		SQLPlayer player = this.playerMap.get(sessionKey);
		if(player == null){
			System.err.println("SQLPlayer thread is not exist for session key '"+sessionKey+"'");
		}else{
			player.postJob(MysqlProtocol.generateComQuitPacket()/* Emulate COM_QUIT command */);
			this.playerMap.remove(sessionKey);
			this.closeSessionCounter.incrementAndGet();
			
			if(MRTEPlayer.IS_DEBUG){
				System.out.println("    >> SQLPlayer["+sourceIp+":"+sourcePort+"] Connection closed");
			}
		}
	}

	protected void processUserRequest(String sourceServerIp, byte[][] parts) throws Exception{
		String sourceIp = ByteHelper.readIpString(parts[0], 0);
		int sourcePort = ByteHelper.readUnsignedShortLittleEndian(parts[1], 0);
		String sessionKey = generateSessionKey(sourceIp, sourcePort);
		
		SQLPlayer player = this.playerMap.get(sessionKey);
		if(player==null){
			// At starting MRTEPlayer, all connection is empty. So we have to guess default database with query
			player = new SQLPlayer(this, sourceIp, sourcePort, this.preparedConnQueue.poll(), this.jdbcUrl, this.mysqlUser, this.mysqlPassword, this.defaultDatabase, this.slowQueryTime, this.replaySelectOnly, SQLPlayer.SESSION_QUEUE_SIZE);
			this.playerMap.put(sessionKey, player);
			player.start();
			this.newSessionCounter.incrementAndGet();
			
			if(MRTEPlayer.IS_DEBUG){
				System.out.println("    >> SQLPlayer["+sourceIp+":"+sourcePort+"] New connection created, query executed without sql player");
			}
		}

		player.postJob(parts[2]);
	}
}
