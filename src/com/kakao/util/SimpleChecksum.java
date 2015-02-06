package com.kakao.util;

public class SimpleChecksum {
	/** largest prime smaller than 65536 */
	private static final int BASE = 65521;


	/**
	 * Updates the checksum with the bytes taken from the array.
	 * 
	 * @param buf
	 *            an array of bytes
	 * @param off
	 *            the start of the data used for this update
	 * @param len
	 *            the number of bytes to use for this update
	 */
	public static long getChecksum(byte[] buf, int off, int len) {
		int checksum = 1;
		
		// (By Per Bothner)
		int s1 = checksum & 0xffff;
		int s2 = checksum >>> 16;

		while (len > 0) {
			// We can defer the modulo operation:
			// s1 maximally grows from 65521 to 65521 + 255 * 3800
			// s2 maximally grows by 3800 * median(s1) = 2090079800 < 2^31
			int n = 3800;
			if (n > len)
				n = len;
			len -= n;
			while (--n >= 0) {
				s1 = s1 + (buf[off++] & 0xFF);
				s2 = s2 + s1;
			}
			s1 %= BASE;
			s2 %= BASE;
		}

		/*
		 * Old implementation, borrowed from somewhere: int n;
		 * 
		 * while (len-- > 0) {
		 * 
		 * s1 = (s1 + (bs[offset++] & 0xff)) % BASE; s2 = (s2 + s1) % BASE; }
		 */

		checksum = (s2 << 16) | s1;
		return (long) checksum & 0xffffffffL;
	}
}