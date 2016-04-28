package com.intel.distml.util;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;

/**
 * Created by yunlong on 4/28/16.
 */
public class DefaultDataWriter implements AbstractDataWriter {


    DataOutputStream dos;

    public DefaultDataWriter(DataOutputStream dos) {
        this.dos = dos;
    }

    public void writeInt(int value) throws Exception {
        dos.writeInt(value);
    }

    public void writeLong(long value) throws Exception {
        dos.writeLong(value);
    }

    public void writeFloat(float value) throws Exception {
        dos.writeFloat(value);

    }

    public void writeDouble(double value) throws Exception {
        dos.writeDouble(value);
    }

    public void writeBytes(byte[] bytes) throws Exception {
        dos.write(bytes);
    }

    public void flush() throws Exception {
        dos.flush();
    }


}
