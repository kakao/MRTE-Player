package com.kakao.mrte;


public class ByteReadException extends Exception{
	public ByteReadException(String funcName, int startPoint, int needLength){
		super(funcName + " : need "+(startPoint + needLength)+" more bytes, but given byte[] is null");
	}
	
	public ByteReadException(String funcName, byte[] data, int startPoint, int needLength){
		super(funcName + " : need "+(startPoint + needLength)+" more bytes, but given byte[] has only " + data.length + " bytes");
	}
}
