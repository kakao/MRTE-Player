package com.kakao.mrte;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.kakao.util.ByteArrayListStream;
import com.kakao.util.SQLHelper;

public class SQLPlayer extends Thread/*implements Runnable*/ {
	
	public final static int SESSION_QUEUE_SIZE = 100;
	
	private boolean isPlayerPrepared = false;
	
	protected final MRTEPlayer parent;
	public final String clientIp;
	public final int clientPort;
	
	public boolean playOnlySelect = false;
	
	public final String jdbcUrl;
	public final String user;
	public final String password;
	public final String defaultDatabase;
	public final long slowQueryTime; /* Milli-second */
	
	private ArrayBlockingQueue<byte[]> jobQueue;
	private ByteArrayListStream stream;
	private Connection targetConnection;
	private Statement stmt;
	private String currentDatabase;
	private AtomicLong connectionOpened;
	private AtomicLong lastQueryExecuted;
	
	
	
	public String getSessionKey(){
		return clientIp + clientPort;
	}
	
	public SQLPlayer(MRTEPlayer parent, String clientIp, int clientPort, Connection conn, String jdbcUrl, String user, String password, String defaultDatabase, long slowQueryTime, boolean playOnlySelect, int queueSize) throws Exception{
		this.parent = parent;
		this.clientIp = clientIp;
		this.clientPort = clientPort;
		
		this.jdbcUrl = jdbcUrl;
		this.user = user;
		this.password = password;
		this.defaultDatabase = defaultDatabase;
		
		this.slowQueryTime = slowQueryTime;
		
		this.playOnlySelect = playOnlySelect;

		this.connectionOpened = new AtomicLong(System.currentTimeMillis());
		this.lastQueryExecuted = new AtomicLong(System.currentTimeMillis());
		
		this.jobQueue = new ArrayBlockingQueue<byte[]>(queueSize);
		this.stream = new ByteArrayListStream(this.jobQueue);
		this.currentDatabase = defaultDatabase;
		
		if(conn==null){
			System.err.println("      SQLPlayer :: create connection on Creator()");
			initConnection(getConnection());
		}else{
			initConnection(conn);
		}
	}
	
	protected Connection getConnection() throws Exception{
		Connection conn = DriverManager.getConnection(this.jdbcUrl, this.user, this.password);
		if(MRTEPlayer.IS_DEBUG){
			System.out.println("    >> SQLPlayer["+this.clientIp+":"+this.clientPort+"] open connection");
		}
		return conn;
	}
	
	protected void initConnection(Connection conn) throws Exception{
		this.targetConnection = conn;
		this.stmt = this.targetConnection.createStatement();
		
		if(this.defaultDatabase!=null && this.defaultDatabase.length()>0){
			this.setDefaultDatabase(this.defaultDatabase);
		}else if(this.currentDatabase!=null && this.currentDatabase.length()>0){
			this.setDefaultDatabase(this.currentDatabase);
		}
		
		this.isPlayerPrepared = true;
		this.connectionOpened.set(System.currentTimeMillis());
	}
	
	protected void reconnect() throws Exception{
		if(checkConnection()){
			return;
		}
		
		if(this.stmt!=null){
			try{this.stmt.close();}catch(Exception ignore){}finally{this.stmt=null;}
		}
		if(this.targetConnection!=null){
			try{this.targetConnection.close();}catch(Exception ignore){}finally{this.targetConnection=null;}
		}
		
		System.err.println("      SQLPlayer :: create connection on reconnect()");
		initConnection(getConnection());
	}
	
	protected boolean checkConnection(){
		ResultSet rs = null;
		try{
			rs = stmt.executeQuery("SELECT 1");
			return true;
		}catch(Exception ex){
			
		}finally{
			if(rs!=null){try{rs.close();}catch(Exception ex){}}
		}
		
		return false;
	}
	
	public void run(){
		MysqlProtocol proto;
		byte[] packet;
		while(true){
			try{
				packet = stream.getPacket();
				if(packet==null){
					continue;
				}
				
				// Parse packet
				try{
					proto = new MysqlProtocol(packet);
				}catch(Exception ex){
					System.err.println("    [ERROR] Parse packet failed : " + ex.getMessage());
					continue;
				}
				
				// Processing packet
				
				if(!isPlayerPrepared){
					try{
						reconnect();
					}catch(Exception ex){
						System.out.println("    >> SQLPlayer["+this.clientIp+":"+this.clientPort+"] Failed to prepare connection");
						ex.printStackTrace(System.err);
					}
				}
				
				try{
					if(proto.command==MysqlProtocol.COM_QUERY){
						if(this.playOnlySelect){
							if(!SQLHelper.isSelectQuery(proto.statement)){
								if(MRTEPlayer.IS_DEBUG){
									System.out.println("    >> SQLPlayer["+this.clientIp+":"+this.clientPort+"] Current mode is only-select, sql '"+proto.statement+"' is ignored");
								}
								continue;
							}
						}
						try{
							long queryStart = System.currentTimeMillis(); // System.nanoTime();
							boolean hasResult = this.stmt.execute(proto.statement);
							long queryEnd = System.currentTimeMillis(); // System.nanoTime();
							if(hasResult){
								ResultSet rs = this.stmt.getResultSet();
								// while(rs.next()); ==> MySQL always buffering all result set from server. so we don't need to iterating all rows.
								rs.close();
							}
							
							if((queryEnd - queryStart) > this.slowQueryTime){
								parent.longQueryCounter.incrementAndGet();
							}
						}catch(Exception ex){
							parent.playerErrorCounter.incrementAndGet();
							if(ex instanceof SQLException){
								SQLException sqle = (SQLException)ex;
								String sqlState = sqle.getSQLState();
								int sqlErrorNo = sqle.getErrorCode();

								if(sqlErrorNo>=2000){ // Server error code
									if(MysqlProtocol.SQLSTATE_NO_INIT_DB.equalsIgnoreCase(sqlState)){
										if(defaultDatabase==null || defaultDatabase.length()<=0){
											findAndSetDefaultDatabase(this.targetConnection, proto.statement);
										}else{
											setDefaultDatabase(this.defaultDatabase);
										}
										parent.noInitDatabsaeCounter.incrementAndGet();
									}else if(MysqlProtocol.SQLSTATE_DUP_KEY.equalsIgnoreCase(sqlState)){
										parent.duplicateKeyCounter.incrementAndGet();
									}else if(MysqlProtocol.SQLSTATE_DEADLOCK.equalsIgnoreCase(sqlState)){
										parent.deadlockCounter.incrementAndGet();
									}else if(MysqlProtocol.SQLERROR_LOCK_WAIT_TIMEOUT==sqlErrorNo){
										parent.lockTimeoutCounter.incrementAndGet();
									}else{
										System.out.println("    >> SQLPlayer["+this.clientIp+":"+this.clientPort+"] Failed to execute sql '"+proto.statement+"'");
									}
								}else{ // Client error code
									try{
										this.reconnect();
									}catch(Exception ex1){
										System.out.println("    >> SQLPlayer["+this.clientIp+":"+this.clientPort+"] Failed to reconnect mysql server (client error during process user request)");
										System.out.println("       " + ex1.getMessage());
										// There's nothing to do more. 
										// Need something. or request stop MRTEPlayer process 
									}
								}
							}
							if(MRTEPlayer.IS_DEBUG){
								System.err.println("    >> SQLPlayer["+this.clientIp+":"+this.clientPort+"] COM_QUERY failed : " + ex.getMessage());
							}
						}
						parent.userRequestCounter.incrementAndGet();
						this.lastQueryExecuted.set(System.currentTimeMillis());
					}else if(proto.command==MysqlProtocol.COM_FIELD_LIST){
						try{
							boolean hasResult = this.stmt.execute("DESC " + proto.statement);
							if(hasResult){
								ResultSet rs = this.stmt.getResultSet();
								// while(rs.next()); ==> MySQL always buffering all result set from server. so we don't need to iterating all rows.
								rs.close();
							}
						}catch(Exception ex){
							// Just ignore this packet
							// parent.playerErrorCounter.incrementAndGet();
							// if(MRTEPlayer.IS_DEBUG){
							// 	System.err.println("    >> SQLPlayer["+this.clientIp+":"+this.clientPort+"] COM_FIELD_LIST failed");
							// 	ex.printStackTrace(System.err);
							// }
						}
						this.lastQueryExecuted.set(System.currentTimeMillis());
						parent.userRequestCounter.incrementAndGet();
					}else if(proto.command==MysqlProtocol.COM_INIT_DB && proto.statement!=null && proto.statement.length()>0){
						boolean result = setDefaultDatabase(proto.statement);
						if(!result){
							System.out.println("    >> SQLPlayer["+this.clientIp+":"+this.clientPort+"] set default db '"+proto.statement+"' failed");
							parent.playerErrorCounter.incrementAndGet();
						}
						
						this.lastQueryExecuted.set(System.currentTimeMillis());
						parent.userRequestCounter.incrementAndGet();
						if(MRTEPlayer.IS_DEBUG){
							System.out.println("    >> SQLPlayer["+this.clientIp+":"+this.clientPort+"] Default db is initialized, and connection is prepared");
						}
					}else if(proto.command==MysqlProtocol.COM_CONNECT){
						// Actually, SQLPlayer can't receive COM_CONNECT packet.
						// So, This is emulated protocol by MysqlProtocol.parse();
						if(proto.statement!=null && proto.statement.length()>0){
							this.setDefaultDatabase(proto.statement);
						}
					}else if(proto.command==MysqlProtocol.COM_QUIT){
						// Stop replaying user request
						isPlayerPrepared = false;
						closeAllResource();
						return; // Exit sqlplayer thread loop
					}
				}catch(Exception ex){
					ex.printStackTrace();
					parent.stopAllPlayers();
				}
				
				// Read next packet
				packet = stream.getPacket();
			}catch(Exception ex){
				System.err.println("[ERROR] Uncaught exception : " + ex.getLocalizedMessage());
				ex.printStackTrace(System.err);
			}
		}
	}
	
	protected void findAndSetDefaultDatabase(Connection conn, String sql){
		String dbName = null;
		ResultSet rs = null;
		try{
			String[] dbTableName = SQLHelper.extractFirstTableName(sql);
			if(dbTableName==null){
				System.err.println("Can not parse table name from query ["+sql+"]");
			}else if(dbTableName!=null && dbTableName.length==2 && dbTableName[0]==null/* table referenced without database */){
				rs = stmt.executeQuery("select table_schema, table_name from information_schema.tables where table_schema not in ('information_schema', 'performance_schema', 'mysql'");
				if(rs.next()){
					dbName = rs.getString(0);
				}
				
				if(rs.next()){
					// current table name is used in multiple database
					// just ignore it
					dbName = null;
				}
			}
			
			if(dbName!=null){
				boolean result = setDefaultDatabase(dbName);
				if(!result){
					System.err.println("Finding and set default database failed : finded '" + dbName + "' from ["+sql+"]");
				}
			}
		}catch(Exception ex){
			if(MRTEPlayer.IS_DEBUG){
				System.err.println("Finding and set default database failed : finded '" + dbName + "' from ["+sql+"]");
				ex.printStackTrace(System.err);
			}
			if(rs!=null){try{rs.close();}catch(Exception ignore){}}
		}
	}
	
	protected boolean setDefaultDatabase(String dbName){
		if(dbName!=null){
			try{
				stmt.executeUpdate("USE " + dbName);
				this.currentDatabase = dbName;
				this.isPlayerPrepared = true;
				return true;
			}catch(Exception ex1){
				//if(MRTEPlayer.IS_DEBUG){
					System.err.println("Setting default database failed : finded '" + dbName + "'");
					ex1.printStackTrace(System.err);
				//}
				// ignore
			}
		}
		
		return false;
	}
	
	public void closeAllResource(){
		try{
			targetConnection.close();
		}catch(Exception ignore){}
	}
	
	public long getLastQueryExecuted(){
		return this.lastQueryExecuted.get();
	}
	
	public long getConnectionOpened(){
		return this.connectionOpened.get();
	}
	
	public void postJob(byte[] packet){
		this.jobQueue.offer(packet);
	}
}
