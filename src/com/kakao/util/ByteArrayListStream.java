package com.kakao.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ByteArrayListStream {
	final int MAX_ALLOWED_PACKET_LEN = 64 * 1024 - 3; // 64KB
	
	private ArrayBlockingQueue<byte[]> queue;
	
    public ByteArrayListStream(ArrayBlockingQueue<byte[]> queue) {
    	this.queue = queue;
    }
    
    protected byte[] getNextBytes(){
    	byte[] bytes = null;
    	do{
    		try{
    			bytes = this.queue.poll(1000, TimeUnit.MILLISECONDS);
    		}catch(Exception ex){
    			System.err.println("Polling the queue failed : " + ex.getMessage());
    		}
    	}while(bytes==null);
    	
    	return bytes;
    }
    
    public byte[] getPacket(){
    	byte[] bytes = getNextBytes();
    	if(bytes.length<3){
    		System.err.println("[ERROR] Initial packet size is too small (less than 3 bytes)");
    		return null;
    	}
    	
    	int currentPacketSize = 0;
    	try{
    		currentPacketSize = ByteHelper.readUnsignedMediumLittleEndian(bytes, 0);
    	}catch(Exception ignore){}
    	if(currentPacketSize>MAX_ALLOWED_PACKET_LEN){
    		System.err.println("[ERROR] packet size is greater than MAX_ALLOWED_PACKET_LEN("+MAX_ALLOWED_PACKET_LEN+")");
    		return null;
    	}
    	
    	if((bytes.length-3) >= currentPacketSize){
    		// Short circuit
    		return Arrays.copyOfRange(bytes, 3, bytes.length);
    	}
    	
    	int stackedSize = (bytes.length - 3);
    	List<byte[]> currentList = new ArrayList<byte[]>();
        currentList.add(Arrays.copyOfRange(bytes, 3, bytes.length));
        
    	while(stackedSize<currentPacketSize){
    		bytes = getNextBytes();
    		stackedSize += bytes.length;
    		currentList.add(bytes);
    	}
    	
    	// Fill complete packet
    	int offset = 0;
    	byte[] packet = new byte[stackedSize];
    	for(int idx=0; idx<currentList.size(); idx++){
    		byte[] p = currentList.get(idx);
    		System.arraycopy(p, 0, packet, offset, p.length);
    		offset += p.length;
    	}
    	
    	return packet;
    }
    
    public static void main(String[] args) throws Exception{
    	// 1 bytes array
    	System.out.println(">>>>>>>>>> 1 packet test");
    	byte[] b = new byte[1024];
    	
    	b[0] = (byte)253;
    	b[1] = (byte)3;
    	b[2] = (byte)0;
    	b[3] = 'a'; b[4] = 'b'; b[5] = 'c';
    	
    	ArrayBlockingQueue<byte[]> queue = new ArrayBlockingQueue<byte[]>(50);
    	ByteArrayListStream stream = new ByteArrayListStream(queue);
    	
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
    	
    	a[0] = (byte)253;
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
    }
}
