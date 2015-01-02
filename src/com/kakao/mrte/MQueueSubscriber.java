package com.kakao.mrte;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.kakao.util.HexDumper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

public class MQueueSubscriber extends Thread/* implements Runnable*/{
	public AtomicBoolean stopSubscriber = new AtomicBoolean(false);
	MRTEPlayer parent;
	
	ConnectionFactory factory;
	Connection connection;
	Channel channel = null;
	QueueingConsumer consumer;
	
	String queueName;
	String exchangeName;
	String exchangeType;
	
	public MQueueSubscriber(MRTEPlayer parent, String mqHost, String mqUser, String mqPassword, int mqPort, String queueName, String exchangeName, String exchangeType) throws Exception{
		this.parent = parent;
		this.queueName = queueName;
		this.exchangeName = exchangeName;
		this.exchangeType = exchangeType;
		
	    factory = new ConnectionFactory();
	    factory.setHost(mqHost);
	    factory.setUsername(mqUser);
	    factory.setPassword(mqPassword);
	    factory.setPort(mqPort);
	    connection = factory.newConnection();

	    reconnect();
	}
	
	protected void reconnect() throws Exception{
		if(this.channel!=null){
			try{
				this.channel.close();
			}catch(Exception ignore){}finally{this.channel=null;}
		}
		
	    channel = connection.createChannel();
	    channel.queueDeclare(this.queueName, false, false, false, null);
	    
	    consumer = new QueueingConsumer(channel);
	    channel.basicConsume(this.queueName, true/*Auto-Ack*/, consumer);
	}
	
	public void run(){
		QueueingConsumer.Delivery delivery;
		while (true) {
			if(stopSubscriber.get()){
				closeAllResource();
				return;
			}
			
			try{
				delivery = consumer.nextDelivery(500 /* Milli-seconds */);
			}catch(Exception ignore){
				System.out.println("RabbitMQ : popup queue failed : " + ignore.getMessage());
				try{
					reconnect();
				}catch(Exception ex){
					System.out.println("RabbitMQ : Can't re-initialize rabbit mq connection (channel)");
					ex.printStackTrace();
					parent.stopAllPlayers();
					return;
				}
				continue;
			}
			
			if(delivery==null){ // timeout, so continue polling queue
				continue;
			}
			
			byte[] payload = delivery.getBody(); // ip(4) + port(2) + mysql_packet_payload
			try{
				List<byte[]> partList = splitParts(payload);
				short protocolSequence = MysqlProtocol.getProtocolSequence(partList.get(2), partList.get(3));
				byte mysqlCommand = MysqlProtocol.getMysqlCommand(partList.get(2), partList.get(3));
				if(protocolSequence!=0/* if (Handshake-Phase) */){
					// Just ignoring it at this time.
				}else if(mysqlCommand==MysqlProtocol.COM_CONNECT){
					parent.processNewSession(partList);
				}else{
					parent.processUserRequest(partList);
				}
			}catch(Exception ignore){
				System.out.println("MQ message processing failed : " + ignore.getMessage());
				System.out.println("---------------------------------------------------------------------");
				HexDumper.dumpBytes(payload);
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
	
	public void closeAllResource(){
		try{
			this.connection.close();
		}catch(Exception ignore){}
	}
}
