package com.kakao.mrte;

import java.util.HashMap;
import java.util.Map;

import com.kakao.util.ByteHelper;


public class MysqlProtocol {
	public final static String SQLSTATE_NO_INIT_DB = "3D000";
	public final static String SQLSTATE_DUP_KEY = "23000";
	public final static String SQLSTATE_DEADLOCK = "40001";
	public final static int    SQLERROR_LOCK_WAIT_TIMEOUT = 1205;
	
	public final static byte COM_SLEEP               = 0x00;
	public final static byte COM_QUIT                = 0x01;
	public final static byte COM_INIT_DB             = 0x02;
	public final static byte COM_QUERY               = 0x03;
	public final static byte COM_FIELD_LIST          = 0x04;
	public final static byte COM_CREATE_DB           = 0x05;
	public final static byte COM_DROP_DB             = 0x06;
	public final static byte COM_REFRESH             = 0x07;
	public final static byte COM_SHUTDOWN            = 0x08;
	public final static byte COM_STATISTICS          = 0x09;
	public final static byte COM_PROCESS_INFO        = 0x0a;
	public final static byte COM_CONNECT             = 0x0b;
	public final static byte COM_PROCESS_KILL        = 0x0c;
	public final static byte COM_DEBUG               = 0x0d;
	public final static byte COM_PING                = 0x0e;
	public final static byte COM_TIME                = 0x0f;
	public final static byte COM_DELAYED_INSERT      = 0x10;
	public final static byte COM_CHANGE_USER         = 0x11;
	public final static byte COM_BINLOG_DUMP         = 0x12;
	public final static byte COM_TABLE_DUMP          = 0x13;
	public final static byte COM_CONNECT_OUT         = 0x14;
	public final static byte COM_REGISTER_SLAVE      = 0x15;
	public final static byte COM_STMT_PREPARE        = 0x16;
	public final static byte COM_STMT_EXECUTE        = 0x17;
	public final static byte COM_STMT_SEND_LONG_DATA = 0x18;
	public final static byte COM_STMT_CLOSE          = 0x19;
	public final static byte COM_STMT_RESET          = 0x1a;
	public final static byte COM_SET_OPTION          = 0x1b;
	public final static byte COM_STMT_FETCH          = 0x1c;
	
	public final static byte COM_UNKNOWN             = 0x7f;
	
	public static Map<Integer, String> COMMAND_MAP = new HashMap<Integer, String>();
	static{
		COMMAND_MAP.put(new Integer(0x00), "COM_SLEEP");
		COMMAND_MAP.put(new Integer(0x01), "COM_QUIT");
		COMMAND_MAP.put(new Integer(0x02), "COM_INIT_DB");
		COMMAND_MAP.put(new Integer(0x03), "COM_QUERY");
		COMMAND_MAP.put(new Integer(0x04), "COM_FIELD_LIST");
		COMMAND_MAP.put(new Integer(0x05), "COM_CREATE_DB");
		COMMAND_MAP.put(new Integer(0x06), "COM_DROP_DB");
		COMMAND_MAP.put(new Integer(0x07), "COM_REFRESH");
		COMMAND_MAP.put(new Integer(0x08), "COM_SHUTDOWN");
		COMMAND_MAP.put(new Integer(0x09), "COM_STATISTICS");
		COMMAND_MAP.put(new Integer(0x0a), "COM_PROCESS_INFO");
		COMMAND_MAP.put(new Integer(0x0b), "COM_CONNECT");
		COMMAND_MAP.put(new Integer(0x0c), "COM_PROCESS_KILL");
		COMMAND_MAP.put(new Integer(0x0d), "COM_DEBUG");
		COMMAND_MAP.put(new Integer(0x0e), "COM_PING");
		COMMAND_MAP.put(new Integer(0x0f), "COM_TIME");
		COMMAND_MAP.put(new Integer(0x10), "COM_DELAYED_INSERT");
		COMMAND_MAP.put(new Integer(0x11), "COM_CHANGE_USER");
		COMMAND_MAP.put(new Integer(0x12), "COM_BINLOG_DUMP");
		COMMAND_MAP.put(new Integer(0x13), "COM_TABLE_DUMP");
		COMMAND_MAP.put(new Integer(0x14), "COM_CONNECT_OUT");
		COMMAND_MAP.put(new Integer(0x15), "COM_REGISTER_SLAVE");
		COMMAND_MAP.put(new Integer(0x16), "COM_STMT_PREPARE");
		COMMAND_MAP.put(new Integer(0x17), "COM_STMT_EXECUTE");
		COMMAND_MAP.put(new Integer(0x18), "COM_STMT_SEND_LONG_DATA");
		COMMAND_MAP.put(new Integer(0x19), "COM_STMT_CLOSE");
		COMMAND_MAP.put(new Integer(0x1a), "COM_STMT_RESET");
		COMMAND_MAP.put(new Integer(0x1b), "COM_SET_OPTION");
		COMMAND_MAP.put(new Integer(0x1c), "COM_STMT_FETCH");
	}
	
	public final static int FLAG_CLIENT_LONG_PASSWORD     = 1;       // new more secure passwords
	public final static int FLAG_CLIENT_FOUND_ROWS        = 2;       // Found instead of affected rows
	public final static int FLAG_CLIENT_LONG_FLAG         = 4;       // Get all column flags
	public final static int FLAG_CLIENT_CONNECT_WITH_DB   = 8;       // One can specify db on connect
	public final static int FLAG_CLIENT_NO_SCHEMA         = 16;      // Don't allow database.table.column
	public final static int FLAG_CLIENT_COMPRESS          = 32;      // Can use compression protocol
	public final static int FLAG_CLIENT_ODBC              = 64;      // Odbc client
	public final static int FLAG_CLIENT_LOCAL_FILES       = 128;     // Can use LOAD DATA LOCAL
	public final static int FLAG_CLIENT_IGNORE_SPACE      = 256;     // Ignore spaces before '('
	public final static int FLAG_CLIENT_PROTOCOL_41       = 512;     // New 4.1 protocol
	public final static int FLAG_CLIENT_INTERACTIVE       = 1024;    // This is an interactive client
	public final static int FLAG_CLIENT_SSL               = 2048;    // Switch to SSL after handshake
	public final static int FLAG_CLIENT_IGNORE_SIGPIPE    = 4096;    // IGNORE sigpipes
	public final static int FLAG_CLIENT_TRANSACTIONS      = 8192;    // Client knows about transactions
	public final static int FLAG_CLIENT_RESERVED          = 16384;   // Old flag for 4.1 protocol
	public final static int FLAG_CLIENT_SECURE_CONNECTION = 32768;   // New 4.1 authentication
	public final static int FLAG_CLIENT_MULTI_STATEMENTS  = 65536;   // Enable/disable multi-stmt support
	public final static int FLAG_CLIENT_MULTI_RESULTS     = 131072;  // Enable/disable multi-results
	public final static int FLAG_CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000; // understands length encoded integer for auth response data in Protocol::HandshakeResponse41
	
	public final static int MYSQL_TYPE_DECIMAL      = 0;
	public final static int MYSQL_TYPE_TINY         = 1;
	public final static int MYSQL_TYPE_SHORT        = 2;
	public final static int MYSQL_TYPE_LONG         = 3;
	public final static int MYSQL_TYPE_FLOAT        = 4;
	public final static int MYSQL_TYPE_DOUBLE       = 5;
	public final static int MYSQL_TYPE_NULL         = 6;
	public final static int MYSQL_TYPE_TIMESTAMP    = 7;
	public final static int MYSQL_TYPE_LONGLONG     = 8;
	public final static int MYSQL_TYPE_INT24        = 9;
	public final static int MYSQL_TYPE_DATE         = 10;
	public final static int MYSQL_TYPE_TIME         = 11;
	public final static int MYSQL_TYPE_DATETIME     = 12;
	public final static int MYSQL_TYPE_YEAR         = 13;
	public final static int MYSQL_TYPE_NEWDATE      = 14;
	public final static int MYSQL_TYPE_VARCHAR      = 15;
	public final static int MYSQL_TYPE_BIT          = 16;
	public final static int MYSQL_TYPE_NEWDECIMAL   = 246;
	public final static int MYSQL_TYPE_ENUM         = 247;
	public final static int MYSQL_TYPE_SET          = 248;
	public final static int MYSQL_TYPE_TINY_BLOB    = 249;
	public final static int MYSQL_TYPE_MEDIUM_BLOB  = 250;
	public final static int MYSQL_TYPE_LONG_BLOB    = 251;
	public final static int MYSQL_TYPE_BLOB         = 252;
	public final static int MYSQL_TYPE_VAR_STRING   = 253;
	public final static int MYSQL_TYPE_STRING       = 254;
	public final static int MYSQL_TYPE_GEOMETRY     = 255;
	
	
	public final static String NETWORK_CHARACTERSET = "UTF-8";
	
	public enum PROTO_PHASE {HANDSHAKE, COMMAND};

	public final PROTO_PHASE phase;
	public final byte command;
	public final short sequence;
	public String statement;
	
	public MysqlProtocol(byte c, short s, String st){
		this.command = c;
		this.sequence = s;
		this.statement = st;

		// In the command phase, the client sends a command packet with the sequence-id [00]:
		this.phase = (this.sequence==0) ? PROTO_PHASE.COMMAND : PROTO_PHASE.HANDSHAKE;
	}
	

	public static short getProtocolSequence(byte[] header, byte[] body){
		if(header==null || header.length<4){
			return -1;
		}
		
		return (short)header[3];
	}
	
	public static byte getMysqlCommand(byte[] header, byte[] body){
		if(header==null || header.length<5){
			return COM_UNKNOWN;
		}
		
		return header[4];
	}
	
	public static void main(String[] args) throws Exception{
		byte[] header = new byte[]{0x14, 0x00, 0x00, 0x03, 0x06};
		byte[] body = new byte[]{(byte)0xee, 0x1d, (byte)0xf5, 0x5f, 0x2d, (byte)0xc9, 0x6a, (byte)0x98, 0x21, 0x44, 0x3b, 0x14, (byte)0xb3, (byte)0xde, (byte)0xff, (byte)0xe1, (byte)0xb6, (byte)0xb0, 0x30};
		System.out.println("body : " + new String(body));
		MysqlProtocol.parse(header, body);
	}
	
	public static MysqlProtocol parse(byte[] header, byte[] body) throws Exception{
		if(header==null || header.length<5){
			throw new Exception("MySQL packet must be greater than 4 bytes");
		}
		
		int userdataSize = ByteHelper.readUnsignedMediumLittleEndian(header, 0);
		if(userdataSize<=0){
			throw new Exception("User data is empty, userdata_size is 0");
		}
		if(userdataSize > 10*1024*1024){
			throw new Exception("User data is exceed 10MB");
		}

		byte command;
		short sequence = (short)(header[3]);
		String statement = null;
		if(sequence==1){
			command = COM_CONNECT; // Fake command
			/**
		     * VERSION 4.1
		     *  Bytes                        Name
		     *  -----                        ----
		     *  4                            client_flags
		     *  4                            max_packet_size
		     *  1                            charset_number
		     *  23                           (filler) always 0x00...
		     *  n (Null-Terminated String)   user
		     *  n (Length Coded Binary)      scramble_buff (1 + x bytes)
		     *  n (Null-Terminated String)   databasename (optional)
		     */
			byte[] compatibilityFlag = new byte[4];
			compatibilityFlag[0] = header[4];
			compatibilityFlag[1] = body[0];
			compatibilityFlag[2] = body[1];
			compatibilityFlag[3] = body[2];
			
			int flag = (int)ByteHelper.readUnsignedIntLittleEndian(compatibilityFlag, 0);
			
			int bytePosition = 4/*flag-length*/-1/*first byte of flag is not stored in body*/;
			if((flag & FLAG_CLIENT_PROTOCOL_41) == FLAG_CLIENT_PROTOCOL_41){
				statement = MysqlProtocol.parseInitDatabaseName(flag, body, bytePosition);
			}else{
				System.err.println("I can only understand mysql client 4.1 and over");
			}
		}else if(sequence>1){
			throw new Exception("Packet sequence number is too high (" + sequence + ")");
		}else{
			command = header[4];
			if(command==COM_INIT_DB || command==COM_QUERY || command==COM_FIELD_LIST){
				statement = (body==null) ? null : new String(body, NETWORK_CHARACTERSET);
				statement = (statement==null) ? null : statement.trim();
			}else if(command==COM_STMT_PREPARE || command==COM_STMT_EXECUTE){
				System.err.println("[ERROR] PrepredStatement is not supported yet, Just ignored");
			}else{
				System.err.println("[INFO]  "+COMMAND_MAP.get(new Integer(command))+" command is not handled in MRTE, Just ignored");
			}
		}
		
		return new MysqlProtocol(command, sequence, statement);
	}
	
	/**
	 * parse init database name from auth-response packet
	 * 
	 * @param compatibilityFlag
	 * @param body
	 * @param offset
	 * @return
	 * @throws Exception
	 */
	protected static String parseInitDatabaseName(int compatibilityFlag, byte[] body, int offset) throws Exception{
		String databaseName = null;
		
		// Read user name
		int index = offset + (4/*MaxPacketSize*/+1/*CharacterSet*/+23/*Reserved*/);
		byte[] userName = ByteHelper.readNullTerminatedBytes(body, index);
		
		// Read auth response
		index += (userName.length+1/*NULL-TERMINATION*/);
		if((compatibilityFlag & FLAG_CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) == FLAG_CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA){
			long[] lengths = ByteHelper.readLengthCodedBinary2(body, index);
			index += (lengths[0]/*length-bytes*/ + lengths[1]/*data-bytes*/);
		}else if((compatibilityFlag & FLAG_CLIENT_SECURE_CONNECTION) == FLAG_CLIENT_SECURE_CONNECTION){
			int length = body[index];
			index += (1 + length);
		}else{
			byte[] authResponse = ByteHelper.readNullTerminatedBytes(body, index);
			index += (authResponse.length+1/*NULL-TERMINATION*/); 
		}
		
		if((compatibilityFlag & FLAG_CLIENT_CONNECT_WITH_DB) == FLAG_CLIENT_CONNECT_WITH_DB){
			byte[] initDatabase = ByteHelper.readNullTerminatedBytes(body, index);
			databaseName = (initDatabase!=null && initDatabase.length>0) ? new String(initDatabase) : null;
		}
		
		return databaseName;
	}
}
