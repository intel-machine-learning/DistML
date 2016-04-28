package com.intel.distml.util;

import java.nio.ByteBuffer;

/**
 * Created by yunlong on 4/28/16.
 */
public class ByteBufferDataWriter implements AbstractDataWriter {


    ByteBuffer buf;

    public ByteBufferDataWriter(ByteBuffer buf) {
        this.buf = buf;
    }

    public void writeInt(int value) throws Exception {
        buf.putInt(value);
    }

    public void writeLong(long value) throws Exception {
        buf.putLong(value);
    }

    public void writeFloat(float value) throws Exception {
        buf.putFloat(value);

    }

    public void writeDouble(double value) throws Exception {
        buf.putDouble(value);
    }

    public void writeBytes(byte[] bytes) throws Exception {
        buf.put(bytes);
    }

    public void flush() throws Exception {

    }

}
