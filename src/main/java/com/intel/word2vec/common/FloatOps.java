package com.intel.word2vec.common;

/**
 * Created by spark on 5/19/14.
 */
public class FloatOps {


    public static byte[] getFloatBytes(float fvalue) {
        byte[] data = new byte[4];
        int ivalue = Float.floatToIntBits(fvalue);
        data[3] = (byte) ((ivalue >> 24) & 0xff);
        data[2] = (byte) ((ivalue >> 16) & 0xff);
        data[1] = (byte) ((ivalue >>  8) & 0xff);
        data[0] = (byte) (ivalue & 0xff);
        return data;
    }

    public static void getFloatBytes(float fvalue, byte[] data, int offset) {
        int ivalue = Float.floatToIntBits(fvalue);
        data[offset+3] = (byte) ((ivalue >> 24) & 0xff);
        data[offset+2] = (byte) ((ivalue >> 16) & 0xff);
        data[offset+1] = (byte) ((ivalue >>  8) & 0xff);
        data[offset] = (byte) (ivalue & 0xff);
        //System.out.println("getFloatBytes: " + ivalue);
    }


    public static float getFloatFromBytes(byte[] data, int offset) {
        int ivalue = ((data[offset+3] << 24) & 0xff000000) + ((data[offset+2] << 16)&0x00ff0000) + ((data[offset+1] << 8)&0x0000ff00) + (data[offset] & 0xff);
        //System.out.println("getFloatFromBytes: " + ivalue);
        return Float.intBitsToFloat(ivalue);
    }

}
