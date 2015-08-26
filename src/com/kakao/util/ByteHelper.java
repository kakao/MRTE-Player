package com.kakao.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.kakao.mrte.ByteReadException;

public class ByteHelper {
	public static final long NULL_LENGTH = -1;
	public static final byte NULL_TERMINATION   = 0x00;
	
	
	

	
	public static String readIpString(byte[] ip, int idx) throws ByteReadException{
		if(ip==null) throw new ByteReadException("ByteHelper.readIpString()", idx, 4);
		if(ip.length<4) throw new ByteReadException("ByteHelper.readIpString()", ip, idx, 4);

		return String.valueOf(ip[idx+0] & 0xFF) + "." +
				String.valueOf(ip[idx+1] & 0xFF) + "." +
				String.valueOf(ip[idx+2] & 0xFF) + "." +
				String.valueOf(ip[idx+3] & 0xFF);
	}
	
	
    public static byte[] readNullTerminatedBytes(byte[] data, int index) throws ByteReadException{
		if(data==null) throw new ByteReadException("ByteHelper.readNullTerminatedBytes()", index, 1);
		if(data.length<=(index+1)) throw new ByteReadException("ByteHelper.readNullTerminatedBytes()", data, index, 1);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int i = index; i < data.length; i++) {
            byte item = data[i];
            if (item == NULL_TERMINATION) {
                break;
            }
            out.write(item);
        }
        return out.toByteArray();
    }
 
    public static byte[] readFixedLengthBytes(byte[] data, int index, int length) throws ByteReadException{
		if(data==null) throw new ByteReadException("ByteHelper.readFixedLengthBytes()", index, length);
		if(data.length<(index+length)) throw new ByteReadException("ByteHelper.readFixedLengthBytes()", data, index, length);

        byte[] bytes = new byte[length];
        System.arraycopy(data, index, bytes, 0, length);
        return bytes;
    }
 
    /**
     * Read 4 bytes in Little-endian byte order.
     * 
     * @param data, the original byte array
     * @param index, start to read from.
     * @return
     */
    public static long readUnsignedIntLittleEndian(byte[] data, int index) throws ByteReadException{
		if(data==null) throw new ByteReadException("ByteHelper.readUnsignedIntLittleEndian()", index, 4);
		if(data.length<(index + 4)) throw new ByteReadException("ByteHelper.readUnsignedIntLittleEndian()", data, index, 4);
    	
        long result = (long) (data[index] & 0xffL) | 
        		(long) ((data[index + 1] & 0xffL) << 8) | 
        		(long) ((data[index + 2] & 0xffL) << 16) | 
        		(long) ((data[index + 3] & 0xffL) << 24);
        return result;
    }
 
    public static long readUnsignedLongLittleEndian(byte[] data, int index) throws ByteReadException{
		if(data==null) throw new ByteReadException("ByteHelper.readUnsignedLongLittleEndian()", index, 8);
		if(data.length<(index + 8)) throw new ByteReadException("ByteHelper.readUnsignedLongLittleEndian()", data, index, 8);

        long accumulation = 0;
        int position = index;
        for (int shiftBy = 0; shiftBy < 64; shiftBy += 8) {
            accumulation |= (long) ((data[position++] & 0xffL) << shiftBy);
        }
        return accumulation;
    }
 
    public static int readUnsignedShortLittleEndian(byte[] data, int index) throws ByteReadException{
		if(data==null) throw new ByteReadException("ByteHelper.readUnsignedShortLittleEndian()", index, 2);
		if(data.length<(index + 2)) throw new ByteReadException("ByteHelper.readUnsignedShortLittleEndian()", data, index, 2);

        int result = (data[index] & 0xFF) | 
        		((data[index + 1] & 0xFF) << 8);
        return result;
    }
 
    public static int readUnsignedMediumLittleEndian(byte[] data, int index) throws ByteReadException{
		if(data==null) throw new ByteReadException("ByteHelper.readUnsignedMediumLittleEndian()", index, 3);
		if(data.length<(index + 3)) throw new ByteReadException("ByteHelper.readUnsignedMediumLittleEndian()", data, index, 3);

        int result = (data[index] & 0xFF) | 
        		((data[index + 1] & 0xFF) << 8) | 
        		((data[index + 2] & 0xFF) << 16);
        return result;
    }
 
    public static long readLengthCodedBinary(byte[] data, int index) throws ByteReadException{
		if(data==null) throw new ByteReadException("ByteHelper.readLengthCodedBinary()", index, 1);
		if(data.length<(index)) throw new ByteReadException("ByteHelper.readLengthCodedBinary()", data, index, 1);
		
        int firstByte = data[index] & 0xFF;
        switch (firstByte) {
            case 251:
                return NULL_LENGTH;
            case 252:
                return readUnsignedShortLittleEndian(data, index + 1);
            case 253:
                return readUnsignedMediumLittleEndian(data, index + 1);
            case 254:
                return readUnsignedLongLittleEndian(data, index + 1);
            default:
                return firstByte;
        }
    }
    
    public static long[] readLengthCodedBinary2(byte[] data, int index) throws ByteReadException{
		if(data==null) throw new ByteReadException("ByteHelper.readLengthCodedBinary2()", index, 1);
		if(data.length<(index)) throw new ByteReadException("ByteHelper.readLengthCodedBinary2()", data, index, 1);
		
        int firstByte = data[index] & 0xFF;
        switch (firstByte) {
            case 251:
                return new long[]{1, NULL_LENGTH};
            case 252:
                return new long[]{2, readUnsignedShortLittleEndian(data, index + 1)};
            case 253:
                return new long[]{3, readUnsignedMediumLittleEndian(data, index + 1)};
            case 254:
                return new long[]{8, readUnsignedLongLittleEndian(data, index + 1)};
            default:
                return new long[]{1, firstByte};
        }
    }
 
    public static byte[] readBinaryCodedLengthBytes(byte[] data, int index) throws ByteReadException, IOException{
		if(data==null) throw new ByteReadException("ByteHelper.readBinaryCodedLengthBytes()", index, 1);
		if(data.length<(index)) throw new ByteReadException("ByteHelper.readBinaryCodedLengthBytes()", data, index, 1);
		
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(data[index]);
 
        byte[] buffer = null;
        int value = data[index] & 0xFF;
        if (value == 251) {
            buffer = new byte[0];
        }
        if (value == 252) {
            buffer = new byte[2];
        }
        if (value == 253) {
            buffer = new byte[3];
        }
        if (value == 254) {
            buffer = new byte[8];
        }
        if (buffer != null) {
        	if(data.length<(index+buffer.length)) throw new ByteReadException("ByteHelper.readBinaryCodedLengthBytes()", data, index, buffer.length);
        	
            System.arraycopy(data, index + 1, buffer, 0, buffer.length);
            out.write(buffer);
        }
 
        return out.toByteArray();
    }
    

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    public static void writeNullTerminatedString(String str, ByteArrayOutputStream out) throws IOException {
        out.write(str.getBytes());
        out.write(NULL_TERMINATION);
    }
 
    public static void writeUnsignedIntLittleEndian(long data, ByteArrayOutputStream out) {
        out.write((byte) (data & 0xFF));
        out.write((byte) (data >>> 8));
        out.write((byte) (data >>> 16));
        out.write((byte) (data >>> 24));
    }
 
    public static void writeUnsignedShortLittleEndian(int data, ByteArrayOutputStream out) {
        out.write((byte) (data & 0xFF));
        out.write((byte) ((data >>> 8) & 0xFF));
    }
 
    public static void writeUnsignedMediumLittleEndian(int data, ByteArrayOutputStream out) {
        out.write((byte) (data & 0xFF));
        out.write((byte) ((data >>> 8) & 0xFF));
        out.write((byte) ((data >>> 16) & 0xFF));
    }
 
    public static void writeBinaryCodedLengthBytes(byte[] data, ByteArrayOutputStream out) throws IOException {
        // 1. write length byte/bytes
        if (data.length < 252) {
            out.write((byte) data.length);
        } else if (data.length < (1 << 16L)) {
            out.write((byte) 252);
            writeUnsignedShortLittleEndian(data.length, out);
        } else if (data.length < (1 << 24L)) {
            out.write((byte) 253);
            writeUnsignedMediumLittleEndian(data.length, out);
        } else {
            out.write((byte) 254);
            writeUnsignedIntLittleEndian(data.length, out);
        }
        // 2. write real data followed length byte/bytes
        out.write(data);
    }
 
    public static void writeFixedLengthBytes(byte[] data, int index, int length, ByteArrayOutputStream out) {
        for (int i = index; i < index + length; i++) {
            out.write(data[i]);
        }
    }
 
    public static void writeFixedLengthBytesFromStart(byte[] data, int length, ByteArrayOutputStream out) {
        writeFixedLengthBytes(data, 0, length, out);
    }
}
