package com.intel.distml.util;

/**
 * Created by yunlong on 4/28/16.
 */
public interface AbstractDataWriter {

    void writeInt(int value) throws Exception;

    void writeLong(long value) throws Exception;

    void writeFloat(float value) throws Exception;

    void writeDouble(double value) throws Exception;

    void writeBytes(byte[] bytes) throws Exception;

    void flush() throws Exception;
}
