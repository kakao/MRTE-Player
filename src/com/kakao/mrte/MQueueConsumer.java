package com.kakao.mrte;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.Adler32;

import com.kakao.util.ByteHelper;
import com.kakao.util.HexDumper;
import com.kakao.util.SimpleChecksum;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class MQueueConsumer extends DefaultConsumer{
	final int PRE_FETCH = 30;
	final int CHECKSUM_LENGTH = 22; // IP(4) + PORT(2) + MYSQL_HEADER(5) + a
	
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
			currPosition += 4;
		}catch(ByteReadException bex){
			System.err.println("[ERROR] Can't read source ip address from packet, body length : " + body.length);
			bex.printStackTrace(System.err);
			return;
		}
		
		Adler32 adler32 = new Adler32();
		String error = null;
		ByteReadException exception = null;
		while(currPosition<body.length){
			if(currPosition+14/* at least, LENGTH(4)+IP(4)+PORT(2)+CHECKSUM(4) */ >= body.length){
				error = "[ERROR] Packet size is too small ("+body.length+" < " + (currPosition+14);
				parent.errorPacketCounter.incrementAndGet();
				break; // no more data or error
			}
			
			// Read current packet-length & packet data
			try{
				packetLen = (int)ByteHelper.readUnsignedIntLittleEndian(body, currPosition); // 4byte packet length
				if(packetLen<=10/* at least, IP(4)+PORT(2)+CHECKSUM(4) */){
					error = "[ERROR] Failed to process MQ message : MQueueConsumer.handleDelivery() - packet length is zero or negative";
					break;
				}
				currPosition += 4;
			}catch(ByteReadException ex){
				error = "[ERROR] Can not read packet size";
				exception = ex;
				break;
			}
			
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
				currPosition += 4/* Checksum */;
				
				adler32.update(packet, 0, (packet.length>=CHECKSUM_LENGTH) ? CHECKSUM_LENGTH : packet.length);
				long calculatedChecksum = adler32.getValue();
				if(checksum!=calculatedChecksum){
					error = "[ERROR] Packet checksum is not correct, calculated-checksum:"+calculatedChecksum+", prebuilt-checksum:"+checksum;
					break;
				}
				adler32.reset();
			}catch(ByteReadException ex){
				error = "[ERROR] Can not read packet checksum";
				exception = ex;
				break;
			}
			
			// Process current packet
			// Packet : ip(4) + port(2) + mysql_packet_payload
			try{
				byte[][] parts = splitMrtePacket(packet);
				if(MysqlProtocol.isComQuitCommand(parts[2])){
					parent.processCloseSession(sourceServerIp, parts);
				}else{
					parent.processUserRequest(sourceServerIp, parts);
				}
				
				parent.recvPacketCounter++;
			}catch(Exception ignore){
				parent.errorPacketCounter.incrementAndGet();
				System.err.println("Failed to process MQ message : " + ignore.getMessage());
				System.err.println("---------------------------------------------------------------------");
				HexDumper.dumpBytes(System.err, packet);
				System.err.println("---------------------------------------------------------------------");
				ignore.printStackTrace(System.err);
			}
		}
		
		if(error!=null){
			parent.errorPacketCounter.incrementAndGet();
			System.err.println(error);
			if(exception!=null) exception.printStackTrace(System.err);
			System.err.println("---------------------------------------------------------------------");
			System.err.println("-- Current position : " + currPosition);
			System.err.println("---------------------------------------------------------------------");
			HexDumper.dumpBytes(System.err, body);
			System.err.println("---------------------------------------------------------------------");
		}
    }
    
	protected byte[][] splitMrtePacket(byte[] payload) throws Exception{
		if(payload.length<(4+2+1/* Mysql packet also contain only 1 byte */)){
			throw new Exception("MQ message must be greater than (4+2+1) bytes");			
		}
		
		byte[] sourceIp = Arrays.copyOfRange(payload, 0, 4);                // ip     : 4 bytes
		byte[] sourcePort = Arrays.copyOfRange(payload, 4, 4+2);            // port   : 2 bytes
		byte[] mysqlProtoBody = (payload.length>(4+2)) ? Arrays.copyOfRange(payload, 4+2, payload.length) : null;  // body : remain...
		
		byte[][] parts = new byte[3][];
		parts[0] = sourceIp;
		parts[1] = sourcePort;
		parts[2] = mysqlProtoBody;
		
		return parts;
	}
}