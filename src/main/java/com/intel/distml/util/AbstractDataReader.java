package com.intel.distml.util;

import java.io.DataInputStream;

/**
 * Created by yunlong on 4/28/16.
 */
public interface AbstractDataReader {

    int readInt() throws Exception;

    long readLong() throws Exception;

    float readFloat() throws Exception;

    double readDouble() throws Exception;

    void waitUntil(int size) throws Exception;

    int readBytes(byte[] bytes) throws Exception;

    void readFully(byte[] bytes) throws Exception;
}
