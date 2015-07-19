package com.kakao.mrte;

import java.util.Arrays;
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
	public final byte sequence;
	public final byte[] packet;
	
	public String statement;
	
	public MysqlProtocol(byte[] packet) throws Exception{
		int currPosition = 0;
		this.sequence = packet[currPosition++];
		//this.command = packet[1];
		this.packet = packet;

		// In the command phase, the client sends a command packet with the sequence-id [00]:
		this.phase = (this.sequence==0) ? PROTO_PHASE.COMMAND : PROTO_PHASE.HANDSHAKE;
		if(sequence>=1 && sequence<=10){
			this.command = COM_CONNECT; // Fake command
			if(packet.length<=5){
				throw new Exception("COM_CONNECT packet's length must be greater than 5, current packet length is " + packet.length);
			}
			
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
			compatibilityFlag[0] = packet[currPosition++];
			compatibilityFlag[1] = packet[currPosition++];
			compatibilityFlag[2] = packet[currPosition++];
			compatibilityFlag[3] = packet[currPosition++];
			
			int flag = (int)ByteHelper.readUnsignedIntLittleEndian(compatibilityFlag, 0);
			
			if((flag & FLAG_CLIENT_PROTOCOL_41) == FLAG_CLIENT_PROTOCOL_41){
				statement = MysqlProtocol.parseInitDatabaseName(flag, packet, currPosition);
			}else{
				statement = "NA";
				throw new Exception("I can only understand mysql client 4.1 and over");
			}
		}else if(sequence>10){
			throw new Exception("Packet sequence number is too high (" + sequence + ")");
		}else{
			command = packet[1];
			if(command==COM_INIT_DB || command==COM_QUERY || command==COM_FIELD_LIST){
				if(packet.length>2){
					statement = new String(Arrays.copyOfRange(packet, 2, packet.length));
					statement = statement.trim();
				}else{
					statement = null;
				}
			}else if(command==COM_QUIT){
				statement = null;
			}else if(command==COM_STMT_PREPARE || command==COM_STMT_EXECUTE){
				throw new Exception("PrepredStatement is not supported yet, Just ignored");
			}else{
				throw new Exception(COMMAND_MAP.get(new Integer(command))+" command is not handled in MRTE, Just ignored");
			}
		}
	}

	public static byte[] generateComQuitPacket(){
		return new byte[]{0x01, 0x00, 0x00, 0x00, MysqlProtocol.COM_QUIT};
	}
	
	public static boolean isComQuitCommand(byte[] packet){
		if(packet!=null && packet.length>=5 && 
				packet[0]==1 &&
				packet[1]==0 &&
				packet[2]==0 &&
				packet[3]==0 /* SequenceNo==0 */ &&
				packet[4]==MysqlProtocol.COM_QUIT)
			return true;
		
		return false;
	}
	
//	public static short getProtocolSequence(byte[] header, byte[] body){
//		if(header==null || header.length<4){
//			return -1;
//		}
//		
//		return (short)header[3];
//	}
//	
//	public static byte getMysqlCommand(byte[] header, byte[] body){
//		if(header==null || header.length<5){
//			return COM_UNKNOWN;
//		}
//		
//		return header[4];
//	}
	
	public static void main(String[] args) throws Exception{
		// Connection try
		byte[] packet1 = new byte[]{
				/*(byte)0x4f, (byte)0x00, (byte)0x00, ==> Length*/ 
				                                    (byte)0x01, (byte)0x8d, (byte)0xa6, (byte)0x0f, (byte)0x20, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x01, (byte)0x21, (byte)0x00, (byte)0x00, (byte)0x00, // |O...... ....!...|
				(byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, // |................|
				(byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x74, (byte)0x65, (byte)0x73, (byte)0x74, (byte)0x5f, (byte)0x75, (byte)0x73, (byte)0x65, (byte)0x72, (byte)0x00, (byte)0x00, (byte)0x74, // |....test_user..t|
				(byte)0x65, (byte)0x73, (byte)0x74, (byte)0x5f, (byte)0x64, (byte)0x61, (byte)0x74, (byte)0x61, (byte)0x62, (byte)0x61, (byte)0x73, (byte)0x65, (byte)0x00, (byte)0x6d, (byte)0x79, (byte)0x73, // |est_database.mys|
				(byte)0x71, (byte)0x6c, (byte)0x5f, (byte)0x6e, (byte)0x61, (byte)0x74, (byte)0x69, (byte)0x76, (byte)0x65, (byte)0x5f, (byte)0x70, (byte)0x61, (byte)0x73, (byte)0x73, (byte)0x77, (byte)0x6f, // |ql_native_passwo|
				(byte)0x72, (byte)0x64, (byte)0x00                                                                                                                                                              // |rd.|
		};
		
		// Query
		byte[] packet2 = new byte[]{
				/*(byte)0x46, (byte)0x00, (byte)0x00, ==> Length */
				                                    (byte)0x00, (byte)0x03, (byte)0x53, (byte)0x45, (byte)0x4c, (byte)0x45, (byte)0x43, (byte)0x54, (byte)0x20, (byte)0x60, (byte)0x64, (byte)0x65, (byte)0x76,  // |F....SELECT `dev|
				(byte)0x69, (byte)0x63, (byte)0x65, (byte)0x73, (byte)0x60, (byte)0x2e, (byte)0x2a, (byte)0x20, (byte)0x46, (byte)0x52, (byte)0x4f, (byte)0x4d, (byte)0x20, (byte)0x60, (byte)0x64, (byte)0x65,  // |ices`.* FROM `de|
				(byte)0x76, (byte)0x69, (byte)0x63, (byte)0x65, (byte)0x73, (byte)0x60, (byte)0x20, (byte)0x57, (byte)0x48, (byte)0x45, (byte)0x52, (byte)0x45, (byte)0x20, (byte)0x28, (byte)0x60, (byte)0x64,  // |vices` WHERE (`d|
				(byte)0x65, (byte)0x76, (byte)0x69, (byte)0x63, (byte)0x65, (byte)0x73, (byte)0x60, (byte)0x2e, (byte)0x75, (byte)0x73, (byte)0x65, (byte)0x72, (byte)0x5f, (byte)0x69, (byte)0x64, (byte)0x20,  // |evices`.user_id |
				(byte)0x3d, (byte)0x20, (byte)0x35, (byte)0x33, (byte)0x33, (byte)0x35, (byte)0x38, (byte)0x35, (byte)0x29, (byte)0x20                                                                           // |= 533585) |
		};
		
		
		byte[] packet3 = new byte[]{
				/*(byte)0x06, (byte)0x00, (byte)0x00, ==> Length*/
				                                    (byte)0x00, (byte)0x03, (byte)0x42, (byte)0x45, (byte)0x47, (byte)0x49, (byte)0x4e // |.....BEGIN|
		};
		
		byte[] packet4 = new byte[]{
				/*(byte)0x07, (byte)0x00, (byte)0x00, ==> Length */
						                            (byte)0x00, (byte)0x03, (byte)0x43, (byte)0x4f, (byte)0x4d, (byte)0x4d, (byte)0x49, (byte)0x54 // |.....COMMIT|
		};
		
		byte[] packet5 = new byte[]{
				/*(byte)0x11, (byte)0x00, (byte)0x00, ==> Length */
				                                    (byte)0x00, (byte)0x03, (byte)0x73, (byte)0x65, (byte)0x74, (byte)0x20, (byte)0x61, (byte)0x75, (byte)0x74, (byte)0x6f, (byte)0x63, (byte)0x6f, (byte)0x6d, //  |.....set autocom|
				(byte)0x6d, (byte)0x69, (byte)0x74, (byte)0x3d, (byte)0x31                                                                                                                                      //  |mit=1|
		};
		
		MysqlProtocol proto = new MysqlProtocol(packet1);
		proto = new MysqlProtocol(packet2);
		proto = new MysqlProtocol(packet3);
		proto = new MysqlProtocol(packet4);
		proto = new MysqlProtocol(packet5);
		
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
		System.out.println(">> UserName : " + new String(userName));
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
			System.out.println(">> Database : " + databaseName);
		}else{
			System.out.println(">> Without Database");
		}
		
		return databaseName;
	}
}
