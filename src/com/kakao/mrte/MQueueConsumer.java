package com.kakao.mrte;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.kakao.util.ByteHelper;
import com.kakao.util.HexDumper;
import com.kakao.util.SimpleChecksum;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class MQueueConsumer extends DefaultConsumer{
	final int PRE_FETCH = 30;
	final int CHECKSUM_LENGTH = 11; // IP(4) + PORT(2) + MYSQL_HEADER(5)
	
	private AtomicBoolean stopSubscriber = new AtomicBoolean(false);
	MRTEPlayer parent;
	String queueName;
	String routingKey;
	Channel channel = null;
    // ExecutorService executorService;
	
    public MQueueConsumer(MRTEPlayer parent, Channel channel, String queueName, String routingKey/*, ExecutorService threadExecutor*/) throws Exception {
    	super(channel);
    	
		this.parent = parent;
		this.routingKey = routingKey;
		this.queueName = queueName;
		
        channel.basicQos(PRE_FETCH);
        channel.basicConsume(queueName, true/* Auto-Ack */, this);
        // executorService = threadExecutor;
	}
    
    public void close(){
    	try{
    		stopSubscriber.set(true);
    		this.channel.close();
    	}catch(Exception ex){}
    }
    
    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
		if(stopSubscriber.get()){
			return;
		}
		if(body==null || body.length<8){
			// packet is empty or something wrong, just ignore
			return;
		}
		
		// MQueue message ==> [packet_len1:4][packet1][[packet_len2:4][packet2][packet_len3:4][packet3]...
		/*
		 * 4-bytes     [4-bytes       4-bytes            2-bytes             variable-length    4-bytes ]...           
		 * LOCAL_IP    [UNIT_LENGTH   PACKET_SOURCE_IP   PACKET_SOURCE_PORT  PACKET_DATA        CHECKSUM]...
		 * 
		 * UNIT_LENGTH = sizeof (PACKET_SOURCE_IP + PACKET_SOURCE_PORT + PACKET_DATA)
		 */
		int currPosition = 0;
		int packetLen = 0;
		byte[] packet;
		
		String sourceServerIp = null;
		try{
			sourceServerIp = ByteHelper.readIpString(body, 0);
		}catch(ByteReadException bex){
			throw new IOException(bex);
		}
		
		currPosition += 4;
		String error = null;
		ByteReadException exception = null;
		while(currPosition<body.length){
			if(currPosition+14/* at least, LENGTH(4)+IP(4)+PORT(2)+CHECKSUM(4) */ >= body.length){
				parent.errorPacketCounter++;
				break; // no more data or error
			}
			
			// Read current packet-length & packet data
			try{
				packetLen = (int)ByteHelper.readUnsignedIntLittleEndian(body, currPosition); // 4byte packet length
				if(packetLen<=10/* at least, IP(4)+PORT(2)+CHECKSUM(4) */){
					error = "[ERROR] Failed to process MQ message : MQueueConsumer.handleDelivery() - packet length is zero or negative";
					break;
				}
			}catch(ByteReadException ex){
				error = "[ERROR] Can not read packet size";
				exception = ex;
				break;
			}
			currPosition += 4;
			
			if( (currPosition + packetLen) > body.length ){
				// There's something wrong.
				error = "[ERROR] Packet size("+packetLen+") is greater than total mq message("+body.length+")";
				break;
			}
			
			packet = new byte[packetLen - 4/* Checksum */];
			System.arraycopy(body, currPosition, packet, 0, packet.length);
			currPosition += packet.length;
			try{
				long checksum = ByteHelper.readUnsignedIntLittleEndian(body, currPosition); // 4byte packet checksum
				long calculatedChecksum = SimpleChecksum.getChecksum(packet, 0, CHECKSUM_LENGTH);
				if(checksum!=calculatedChecksum){
					error = "[ERROR] Packet checksum is not correct, calculated-checksum:"+calculatedChecksum+", prebuilt-checksum:"+checksum;
					break;
				}
			}catch(ByteReadException ex){
				error = "[ERROR] Can not read packet checksum";
				exception = ex;
				break;
			}
			currPosition += 4/* Checksum */;
			
			// Process current packet
			// Packet : ip(4) + port(2) + mysql_packet_payload
			try{
				List<byte[]> partList = splitParts(packet);
				short protocolSequence = MysqlProtocol.getProtocolSequence(partList.get(2), partList.get(3));
				byte mysqlCommand = MysqlProtocol.getMysqlCommand(partList.get(2), partList.get(3));
				if(protocolSequence!=0/* if (Handshake-Phase) */){
					// This is handshake-phase between client and server
					// We don't know this trying is going to succeed or not
					// So just ignoring it
					// And actually, we will never receive MysqlProtocol.COM_CONNECT command using only server-recv packet.
					//               MysqlProtocol.COM_CONNECT is sent from server to client
					//               And client will reponse message with (sequence=1 not 0) to server, if that case we try processNewSession()
					try{
						parent.processNewSession(sourceServerIp, partList);
					}catch(Exception ex){
						if(parent.IS_DEBUG){
							throw ex;
						}
					}
				//}else if(mysqlCommand==MysqlProtocol.COM_CONNECT){
				//	parent.processNewSession(partList);
				//	processedNewSession++;
				}else if(mysqlCommand==MysqlProtocol.COM_QUIT){
					parent.processCloseSession(sourceServerIp, partList);
				}else{
					parent.processUserRequest(sourceServerIp, partList);
				}
				
				parent.recvPacketCounter++;
			}catch(Exception ignore){
				parent.errorPacketCounter++;
				System.err.println("Failed to process MQ message : " + ignore.getMessage());
				System.err.println("---------------------------------------------------------------------");
				HexDumper.dumpBytes(System.err, packet);
				System.err.println("---------------------------------------------------------------------");
				ignore.printStackTrace(System.err);
			}
		}
		
		if(error!=null){
			parent.errorPacketCounter++;
			System.err.println(error);
			if(exception!=null) exception.printStackTrace(System.err);
			System.err.println("---------------------------------------------------------------------");
			System.err.println("-- Current position : " + currPosition);
			System.err.println("---------------------------------------------------------------------");
			HexDumper.dumpBytes(System.err, body);
			System.err.println("---------------------------------------------------------------------");
		}
    }
    
	protected List<byte[]> splitParts(byte[] payload) throws Exception{
		if(payload.length<(4+2+5)){
			throw new Exception("MQ message must be greater than (4+2+5) bytes");			
		}
		
		byte[] sourceIp = Arrays.copyOfRange(payload, 0, 4);                // ip     : 4 bytes
		byte[] sourcePort = Arrays.copyOfRange(payload, 4, 4+2);            // port   : 2 bytes
		byte[] mysqlProtoHeader = Arrays.copyOfRange(payload, 4+2, 4+2+5);  // header : 5 bytes (length:3, sequence:1, command:1)
		byte[] mysqlProtoBody = (payload.length>(4+2+5)) ? Arrays.copyOfRange(payload, 4+2+5, payload.length) : null;  // body : remain...
		
		List<byte[]> partList = new ArrayList<byte[]>();
		partList.add(sourceIp);
		partList.add(sourcePort);
		partList.add(mysqlProtoHeader);
		partList.add(mysqlProtoBody);
		
		return partList;
	}
}