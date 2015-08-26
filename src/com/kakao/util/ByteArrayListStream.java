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
     * @return mysql_sequence + mysql_payload (Does not contain payload_length (3 bytes))
     */
    public byte[] getPacket(){
    	byte[] bytes = getNextBytes();
    	long lastPacketArrivalTimeMs = System.currentTimeMillis();
    	
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
    		return Arrays.copyOfRange(bytes, 3, bytes.length);
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
    		
    		long currentMs = System.currentTimeMillis();
    		if((currentMs - lastPacketArrivalTimeMs) >= 500){
    			// For Debugging
    			// System.err.println("[WARN] splitted packet arrival time gap is over 500 milli-seconds");
    			lastPacketArrivalTimeMs = currentMs;
    		}
    		stackedSize += bytes.length;
    		currentList.add(bytes);
    		
    		if(MysqlProtocol.hasValidMySQLPacketHeader(bytes)){
				// Something wrong, Might be packet is lost somewhere. (I don't now why and where yet)
				// Ignore everything accumulated, and return just current query.
				this.errorPacketCounter.incrementAndGet();
				
    			System.err.println(">> Wrong Packet (Expected : "+currentPacketSize+", Current : "+(stackedSize-4)+")---------------------------");
    			for(int x=0; x<currentList.size(); x++){
    				System.err.println("> Packet : " + x); System.err.flush();
    				HexDumper.dumpBytes(System.err, currentList.get(x));
    				System.err.flush();
    			}
    			
    			// Previous packet is not useless, so drop previous packet and just return current complete packet
    			return Arrays.copyOfRange(bytes, 3, bytes.length);
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
    	
    	return packet;
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
    		
    		byte[] packet = stream.getPacket();
    		if(packet==null) System.out.println("> idx="+idx+" : Packet is null");
    		else{
    			byte[] b1 = new byte[]{packet[0], packet[1], packet[2]};
    			System.out.println("> idx="+idx+" : Packet header : " + new String(b1));
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
    		
    		byte[] packet = stream.getPacket();
    		if(packet==null) System.out.println("> idx="+idx+" : Packet is null");
    		else{
    			byte[] b1 = new byte[]{packet[0], packet[1], packet[2]};
    			System.out.println("> idx="+idx+" : Packet header : " + new String(b1));
    		}
    	}
    	
    	// Real test 
    	System.out.println(">>>>>>>>>> real packet test");
    	byte[] t = new byte[]{0x05, 0x00, 0x00, 0x00, 0x02, 0x74, 0x65, 0x73, 0x74};
    	queue.offer(t);
    	byte[] packet1 = stream.getPacket();
    	System.out.println(packet1[2]);
    }
}
