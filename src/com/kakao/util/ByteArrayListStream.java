package com.kakao.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.kakao.mrte.MysqlProtocol;
import com.kakao.mrte.SQLPlayer;

public class ByteArrayListStream {
	private long maxAllowedPacketSize;
	private ConcurrentLinkedQueue<byte[]> queue;
	protected AtomicLong errorPacketCounter;
	protected AtomicLong dequeuePacketCounter;
	
	// Debug print
	//static Random randomGenerator = new Random();
	//PrintStream debugLogStream = null;
	
	
    public ByteArrayListStream(ConcurrentLinkedQueue<byte[]> queue, AtomicLong errorPacketCounter, AtomicLong dequeuePacketCounter,long maxAllowedPacketSize) {
    	this.queue = queue;
    	this.maxAllowedPacketSize = maxAllowedPacketSize;
    	this.errorPacketCounter = errorPacketCounter;
    	this.dequeuePacketCounter = dequeuePacketCounter;
    	// Debug Print
    	/*
    	try{
    		this.debugLogStream = new PrintStream(new File("dump_" + randomGenerator.nextLong() + ".log"));
    	}catch(Exception ignore){
    		this.debugLogStream = null;
    	}
    	*/
    }
    
    protected byte[] getNextBytes(){
    	byte[] bytes = null;
    	while(bytes==null){
    		bytes = this.queue.poll();
    		if(bytes==null){
    			try{
    				Thread.sleep(10 /* Milli seconds */);
    			}catch(Exception ignore){}
    		}
    	}
    	
    	return bytes;
    }
    
    /**
     * Return completed packet whether it is splitted multiple tcp stream
     * 
     * @return ArrayList of "mysql_sequence + mysql_payload (Does not contain payload_length (3 bytes))"
     */
    public List<byte[]> getPackets(){
    	List<byte[]> packetList = new ArrayList<byte[]>();
    	byte[] bytes = getNextBytes();
    	
    	if(bytes.length<3){
    		this.errorPacketCounter.incrementAndGet();
    		System.err.println("[ERROR] Initial packet size is too small (less than 3 bytes)");
    		return null;
    	}
    	
    	int currentPacketSize = 0;
    	try{
    		// Length of the payload. The number of bytes in the packet beyond the initial 4 bytes that make up the packet header.
    		// SO, (Payload length) == (Total packet length - 4)
    		currentPacketSize = ByteHelper.readUnsignedMediumLittleEndian(bytes, 0);
    	}catch(Exception ignore){}
    	
    	if(currentPacketSize<=0){
    		this.errorPacketCounter.incrementAndGet();
    		System.err.println("[ERROR] packet size is less than 0");
    		return null;
    	}
    	
    	if((currentPacketSize+4) > maxAllowedPacketSize){
    		this.errorPacketCounter.incrementAndGet();
    		System.err.println("[ERROR] packet size is greater than MAX_ALLOWED_PACKET_LEN("+ this.maxAllowedPacketSize+")");
    		return null;
    	}
    	
    	if((bytes.length-4) >= currentPacketSize){
    		// Short circuit
    		packetList.add(Arrays.copyOfRange(bytes, 3, bytes.length));
    		return packetList;
    	}
    	
//    	// Debug printing
//    	try{
//	    	PrintStream tempFile = new PrintStream(new File("dump_" + randomGenerator.nextLong() + ".log"));
//	    	tempFile.println("------------------ ");
//	    	tempFile.println("  Payload length : " + currentPacketSize + ", Total packet length : " + bytes.length);
//	    	HexDumper.dumpBytes(tempFile, bytes);
//	    	tempFile.close();
//    	}catch(Exception ex){
//    		System.out.println("Can not write hex dump : " + ex.getMessage());
//    	}
    	
    	// If we are here, it means that query is splitted into multiple packet
    	int stackedSize = bytes.length;
    	List<byte[]> currentList = new ArrayList<byte[]>();
        currentList.add(bytes);
        
    	while((stackedSize-4) < currentPacketSize){ // stackedSize includes 4 byte header, so we have to compare (stackedSize-4) with mysql payload length
    		bytes = getNextBytes();
    		if(MysqlProtocol.hasValidMySQLPacketHeader(bytes)){
				// There's something wrong in TCP stream.
    			// But actually MRTE-Collector will send "COM_INIT_DB" and "COM_QUIT" command articificially.
    			//     This will break TCP stream. So we have to handle this kind of broken message.
    			packetList.add(Arrays.copyOfRange(bytes, 3, bytes.length));
    			if(packetList.size()>5){
    				// If this broken message happen more than 5 times, just considering this as Real error.
    				return packetList;
    			}
			}else{
	    		stackedSize += bytes.length;
	    		currentList.add(bytes);
			}
    	}
    	
    	// Debug printing
    	/*
		if(this.debugLogStream!=null){
			this.debugLogStream.println("\n-- Accumulating packet : " + currentList.size() + ", Total packet length : " + (stackedSize-4+1));
		}
		*/

    	// Fill complete packet
    	int offset = 0;
    	byte[] packet = new byte[stackedSize-4+1]; // stackedSize includes 4 byte header but we have to return 4th byte (sequence-no)
    	for(int idx=0; idx<currentList.size(); idx++){
    		byte[] p = currentList.get(idx);
    		if(idx==0){
    			// first packet has 4 bytes header(length 3 bytes, sequence 1 byte), we have to return sequence byte but not length 3 bytes
        		System.arraycopy(p, 3, packet, offset, p.length-3);
        		offset += (p.length-3);
    		}else{
	    		System.arraycopy(p, 0, packet, offset, p.length);
	    		offset += p.length;
    		}
    		
        	// Debug printing
    		/*
    		if(this.debugLogStream!=null){
    			HexDumper.dumpBytes(this.debugLogStream, p);
    			this.debugLogStream.println();
    		}
    		*/
    	}
    	
    	packetList.add(packet);
    	return packetList;
    }
    
    public static void main(String[] args) throws Exception{
    	// 1 bytes array
    	System.out.println(">>>>>>>>>> 1 packet test");
    	byte[] b = new byte[1024];
    	
    	b[0] = (byte)252;
    	b[1] = (byte)3;
    	b[2] = (byte)0;
    	b[3] = 'a'; b[4] = 'b'; b[5] = 'c';
    	
    	ConcurrentLinkedQueue<byte[]> queue = new ConcurrentLinkedQueue<byte[]>();
    	ByteArrayListStream stream = new ByteArrayListStream(queue, new AtomicLong(0), new AtomicLong(0), SQLPlayer.MAX_ALLOWED_PACKET_LEN);
    	
    	for(int idx=0; idx<100; idx++){
    		queue.offer(b);
    		
    		List<byte[]> packets = stream.getPackets();
    		if(packets==null) System.out.println("> idx="+idx+" : Packet is null");
    		else{
    			for(int idx1=0; idx1<packets.size(); idx1++){
    				byte[] packet = packets.get(idx1);
    				byte[] b1 = new byte[]{packet[0], packet[1], packet[2]};
    				System.out.println("> idx="+idx+" : Packet header : " + new String(b1));
    			}
    		}
    	}
    	
    	// 2 bytes array
    	System.out.println(">>>>>>>>>> 2 packet test");
    	byte[] a = new byte[1000];
    	b = new byte[24];
    	
    	a[0] = (byte)252;
    	a[1] = (byte)3;
    	a[2] = (byte)0;
    	a[3] = 'a'; a[4] = 'b'; a[5] = 'c';
    	for(int idx=0; idx<100; idx++){
    		queue.offer(a);
    		queue.offer(b);  // Check if blocked when this line is commented.
    		
    		List<byte[]> packets = stream.getPackets();
    		if(packets==null) System.out.println("> idx="+idx+" : Packet is null");
    		else{
    			for(int idx1=0; idx1<packets.size(); idx1++){
    				byte[] packet = packets.get(idx1);
    				byte[] b1 = new byte[]{packet[0], packet[1], packet[2]};
    				System.out.println("> idx="+idx+" : Packet header : " + new String(b1));
    			}
    		}
    	}
    	
    	// Real test 
    	System.out.println(">>>>>>>>>> real packet test");
    	byte[] t = new byte[]{0x05, 0x00, 0x00, 0x00, 0x02, 0x74, 0x65, 0x73, 0x74};
    	queue.offer(t);
    	List<byte[]> packets1 = stream.getPackets();
    	for(int idx=0; idx<packets1.size(); idx++){
    		byte[] packet1 = packets1.get(idx);
    		System.out.println(packet1[2]);
    	}
    }
}
