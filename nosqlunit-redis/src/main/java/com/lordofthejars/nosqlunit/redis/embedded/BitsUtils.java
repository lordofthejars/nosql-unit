package com.lordofthejars.nosqlunit.redis.embedded;

public class BitsUtils {

	public static byte[] extendByteArrayBy(byte[] data, int numberOfNewPositions) {
		
		byte[] newSize = new byte[data.length+numberOfNewPositions];
		System.arraycopy(data, 0, newSize, 0, data.length);
		
		return newSize;
	}
	
	public static boolean toBoolean(int bit) {
		if(bit == 0) {
			return Boolean.FALSE;
		} else {
			return Boolean.TRUE;
		}
	}
	
	public static int countBits(byte num) {
		int count = 0;
		for (int i = 0; i < 8; i++) {
			if ((num & 1) == 1) // check if right most bit is 1
			{
				count++;
			}
			num = (byte) (num >>> 1); // shit right 1 bit, including the sign
										// bit
		}
		return count;
	}

	public static int calculateNumberOfBytes(int numBits) {
		int numberOfBytes = numBits/8;
		
		if(numBits % 8 > 0) {
			numberOfBytes++;
		}
		
		return numberOfBytes;
		
	}
	
	public static int getBit(byte[] data, int pos) {
		int posByte = pos / 8;
		int posBit = pos % 8;
		byte valByte = data[posByte];
		int valInt = valByte >> (8 - (posBit + 1)) & 0x0001;
		return valInt;
	}

	public static void setBit(byte[] data, int pos, int val) {
		int posByte = pos / 8;
		int posBit = pos % 8;
		byte oldByte = data[posByte];
		oldByte = (byte) (((0xFF7F >> posBit) & oldByte) & 0x00FF);
		byte newByte = (byte) ((val << (8 - (posBit + 1))) | oldByte);
		data[posByte] = newByte;
	}

}
