package com.kakao.util;


public class HexDumper {

	public static void dumpBytes(byte[] bytes){
		int width = 16;
		for (int index = 0; index < bytes.length; index += width) {
			try{
				printHex(bytes, index, width);
				printAscii(bytes, index, width);
			}catch(Exception ignore){
				System.out.println("Can't dump byte[], " + ignore.getMessage());
			}
		}
	}

	private static void printHex(byte[] bytes, int offset, int width) {
		for (int index = 0; index < width; index++) {
			if (index + offset < bytes.length) {
				System.out.printf("%02x ", bytes[index + offset]);
			} else {
				System.out.print("	");
			}
		}
	}

	private static void printAscii(byte[] bytes, int index, int width) throws Exception{
		/*
		for (int index = 0; index < width; index++) {
			if (index + offset < bytes.length){
				char c = (char)bytes[index];
				if(c=='\r' || c=='\n' || c=='\t'){
					System.out.print(".");
				}else if (c < 0x30 || (c >= 0x3a && c <= 0x40) || (c > 0x5a && c <= 0x60) || c > 0x7a){
					System.out.print(".");
				}else{
					System.out.printf("%c", (char)bytes[index + offset]);
				}
			}else{
				break;
			}
		}
		
		System.out.println();
		*/
		
		
		if (index < bytes.length) {
			width = Math.min(width, bytes.length - index);
			System.out.println(":" + new String(bytes, index, width, "US-ASCII").replaceAll("\r\n", " ").replaceAll("\n"," "));
		} else {
			System.out.println();
		}
	}
}