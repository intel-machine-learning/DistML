package com.intel.distml.util;

import java.nio.ByteBuffer;

/**
 * Created by yunlong on 4/28/16.
 */
public class ByteBufferDataReader implements AbstractDataReader {

    ByteBuffer buf;

    public ByteBufferDataReader(ByteBuffer buf) {
        this.buf = buf;
    }

    public int readInt() {
        return buf.getInt();
    }

    public long readLong() {
        return buf.getLong();
    }

    public float readFloat() {
        return buf.getFloat();
    }

    public double readDouble() {
        return buf.getDouble();
    }

    public int readBytes(byte[] bytes) throws Exception {
        buf.get(bytes);

        return bytes.length;
    }

    public void readFully(byte[] bytes) throws Exception {
        buf.get(bytes);
    }


    public void waitUntil(int size) throws Exception {

    }

}
