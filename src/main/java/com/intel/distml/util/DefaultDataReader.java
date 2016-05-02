package com.intel.distml.util;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by yunlong on 4/28/16.
 */
public class DefaultDataReader implements AbstractDataReader {

    DataInputStream dis;

    public DefaultDataReader(DataInputStream dis) {
        this.dis = dis;
    }

    public int readInt() throws Exception {
        return dis.readInt();
    }

    public long readLong() throws Exception {
        return dis.readLong();
    }

    public float readFloat() throws Exception {
        return dis.readFloat();
    }

    public double readDouble() throws Exception {
        return dis.readDouble();
    }

    public int readBytes(byte[] bytes) throws Exception {
        return dis.read(bytes);
    }

    public void readFully(byte[] bytes) throws Exception {
        dis.readFully(bytes);
    }

    public void waitUntil(int size) throws Exception {
        while(dis.available() < size) { try { Thread.sleep(1); } catch (Exception e){}}
    }

}
