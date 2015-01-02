package com.kakao.mrte;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.kakao.util.ByteHelper;
import com.kakao.util.HexDumper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class MQueueConsumer extends DefaultConsumer{
	final int PRE_FETCH = 30;
	
	private AtomicBoolean stopSubscriber = new AtomicBoolean(false);
	MRTEPlayer parent;
	String queueName;
	Channel channel = null;
    // ExecutorService executorService;
	
    public MQueueConsumer(MRTEPlayer parent, Channel channel, String queueName/*, ExecutorService threadExecutor*/) throws Exception {
    	super(channel);
    	
		this.parent = parent;
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
		if(body==null || body.length<4){
			// packet is empty or something wrong, just ignore
			return;
		}
		
		// MQueue message ==> [packet_len1:4][packet1][[packet_len2:4][packet2][packet_len3:4][packet3]...
		int currPosition = 0;
		int packetLen = 0;
		byte[] packet;
		while(currPosition<body.length){
			if(currPosition+4 >= body.length){
				parent.errorPacketCounter++;
				break; // no more data or error
			}
			
			// Read current packet-length & packet data
			try{
				packetLen = (int)ByteHelper.readUnsignedIntLittleEndian(body, currPosition); // 4byte packet length
			}catch(ByteReadException ex){
				System.err.println("[ERROR] Can not read packet size");
				ex.printStackTrace(System.err);
				parent.errorPacketCounter++;
				break;
			}
			currPosition += 4;
			
			if( (currPosition + packetLen) > body.length ){
				// There's something wrong.
				parent.errorPacketCounter++;
				System.err.println("[ERROR] Packet size("+packetLen+") is greater than total mq message("+body.length+")");
				break; // Stop & ignore remain data
			}
			packet = new byte[packetLen];
			System.arraycopy(body, currPosition, packet, 0, packet.length);
			currPosition += packet.length;
			
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
					
					parent.processNewSession(partList);
				//}else if(mysqlCommand==MysqlProtocol.COM_CONNECT){
				//	parent.processNewSession(partList);
				//	processedNewSession++;
				}else if(mysqlCommand==MysqlProtocol.COM_QUIT){
					parent.processCloseSession(partList);
				}else{
					parent.processUserRequest(partList);
				}
				
				parent.recvPacketCounter++;
			}catch(Exception ignore){
				parent.errorPacketCounter++;
				System.out.println("Failed to process MQ message : " + ignore.getMessage());
				System.out.println("---------------------------------------------------------------------");
				HexDumper.dumpBytes(packet);
				System.out.println("---------------------------------------------------------------------");
				ignore.printStackTrace();
			}
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