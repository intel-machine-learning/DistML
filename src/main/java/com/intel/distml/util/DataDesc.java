package com.intel.distml.util;

import java.io.*;

/**
 * Created by yunlong on 12/30/15.
 */
public final class DataDesc implements Serializable {

    public static final int DATA_TYPE_ARRAY       = 0;
    public static final int DATA_TYPE_MATRIX      = 1;

    public static final int KEY_TYPE_INT          = 0;
    public static final int KEY_TYPE_LONG         = 1;

    public static final int ELEMENT_TYPE_INT      = 0;
    public static final int ELEMENT_TYPE_FLOAT    = 1;
    public static final int ELEMENT_TYPE_LONG     = 2;
    public static final int ELEMENT_TYPE_DOUBLE   = 3;

    public int dataType;
    public int keyType;
    public int valueType;

    public boolean denseRow;
    public boolean denseColumn;
    public boolean adaGrad;

    public int keySize;
    public int valueSize;

    public DataDesc() {
    }

    public DataDesc(int dataType, int keyType, int valueType) {
        this(dataType, keyType, valueType, false, true);
    }

    public DataDesc(int dataType, int keyType, int valueType, boolean denseRow, boolean denseColumn) {
        this(dataType, keyType, valueType, denseRow, denseColumn, false);
    }
    public DataDesc(int dataType, int keyType, int valueType, boolean denseRow, boolean denseColumn, boolean adaGrade) {
        this.dataType = dataType;
        this.valueType = valueType;
        this.keyType = keyType;
        this.denseRow = denseRow;
        this.denseColumn = denseColumn;
        this.adaGrad = adaGrade;

        this.keySize = (keyType == KEY_TYPE_INT)? 4 : 8;
        this.valueSize = ((valueType == ELEMENT_TYPE_INT) || (valueType == ELEMENT_TYPE_FLOAT))? 4 : 8;
    }

    public String toString() {
        return "" + dataType + ", " + keyType + ", " + keySize + ", " + valueType + ", " + valueSize;
    }

    public int sizeAsBytes() {
        return 24;  // keySize and valueSize are calculated in fly
    }

    public void write(AbstractDataWriter out) throws Exception {
        out.writeInt(dataType);
        out.writeInt(keyType);
        out.writeInt(valueType);
        out.writeInt(denseRow ? 1 : 0);
        out.writeInt(denseColumn ? 1 : 0);
        out.writeInt(adaGrad ? 1 : 0);
    }

    public void read(AbstractDataReader in) throws Exception {
        dataType = in.readInt();
        keyType = in.readInt();
        valueType = in.readInt();
        denseRow = in.readInt() == 1;
        denseColumn = in.readInt() == 1;
        adaGrad = in.readInt() == 1;

        this.keySize = (keyType == KEY_TYPE_INT)? 4 : 8;
        this.valueSize = ((valueType == ELEMENT_TYPE_INT) || (valueType == ELEMENT_TYPE_FLOAT))? 4 : 8;
    }

    public Number readKey(AbstractDataReader is) throws Exception {
        if (keyType == KEY_TYPE_INT) {
            return is.readInt();
        }
        else {
            return is.readLong();
        }
    }


    public void writeKey(Number v, AbstractDataWriter os) throws Exception {
        if (keyType == KEY_TYPE_INT) {
            os.writeInt(v.intValue());
        }
        else {
            os.writeLong(v.longValue());
        }
    }

    public Object readValue(AbstractDataReader is) throws Exception {
        switch(valueType) {
            case ELEMENT_TYPE_INT:
                return is.readInt();
            case ELEMENT_TYPE_FLOAT:
                return is.readFloat();
            case ELEMENT_TYPE_LONG:
                return is.readLong();
            case ELEMENT_TYPE_DOUBLE:
                return is.readDouble();
        }

        throw new IllegalStateException("invalid value type: " + valueType);
    }

    public void writeValue(Object value, AbstractDataWriter os) throws Exception {
        switch(valueType) {
            case ELEMENT_TYPE_INT:
                os.writeInt((Integer) value);
            case ELEMENT_TYPE_FLOAT:
                os.writeFloat((Float) value);
            case ELEMENT_TYPE_LONG:
                os.writeLong((Long) value);
            case ELEMENT_TYPE_DOUBLE:
                os.writeDouble((Double)value);
        }
        throw new IllegalStateException("invalid value type: " + valueType);
    }

    public Number readKey(byte[] data, int offset) {
        if (keyType == KEY_TYPE_INT) {
            return readInt(data, offset);
        }
        else {
            return readLong(data, offset);
        }
    }

    public int writeKey(Number v, byte[] data, int offset) {
        if (keyType == KEY_TYPE_INT) {
            write(v.intValue(), data, offset);
            return offset + 4;
        }
        else {
            write(v.longValue(), data, offset);
            return offset + 8;
        }
    }

    public Object readValue(byte[] buf, int offset) {
        switch(valueType) {
            case ELEMENT_TYPE_INT:
                return readInt(buf, offset);
            case ELEMENT_TYPE_FLOAT:
                return readFloat(buf, offset);
            case ELEMENT_TYPE_LONG:
                return readLong(buf, offset);
            case ELEMENT_TYPE_DOUBLE:
                return readDouble(buf, offset);
        }

        throw new IllegalStateException("invalid value type: " + valueType);
    }

    public int writeValue(Object value, byte[] buf, int offset) {
        switch(valueType) {
            case ELEMENT_TYPE_INT:
                return write((Integer)value, buf, offset);
            case ELEMENT_TYPE_FLOAT:
                return write((Float)value, buf, offset);
            case ELEMENT_TYPE_LONG:
                return write((Long)value, buf, offset);
            case ELEMENT_TYPE_DOUBLE:
                return write((Double)value, buf, offset);
        }
        throw new IllegalStateException("invalid value type: " + valueType);
    }

    public int readInt(byte[] data, int offset) {
        int targets =
                          (data[offset  ]         & 0x000000ff)
                        | ((data[offset+1] << 8)  & 0x0000ff00)
                        | ((data[offset+2] << 16) & 0x00ff0000)
                        | ((data[offset+3] << 24) & 0xff000000);

        return targets;
    }
    public float readFloat(byte[] data, int offset) {
        int targets = readInt(data, offset);
        return Float.intBitsToFloat(targets);
    }

    public long readLong(byte[] data, int offset) {
        long targets =
                          (data[offset  ]         & 0x00000000000000ffL)
                        | ((data[offset+1] << 8)  & 0x000000000000ff00L)
                        | ((data[offset+2] << 16) & 0x0000000000ff0000L)
                        | ((data[offset+3] << 24) & 0x00000000ff000000L)
                        | ((((long)data[offset+4]) << 32) & 0x000000ff00000000L)
                        | ((((long)data[offset+5]) << 40) & 0x0000ff0000000000L)
                        | ((((long)data[offset+6] << 48)) & 0x00ff000000000000L)
                        | ((((long)data[offset+7] << 56)) & 0xff00000000000000L);

        return targets;
    }
    public double readDouble(byte[] data, int offset) {
        long targets = readLong(data, offset);
        double value = Double.longBitsToDouble(targets);
        //System.out.println("read double: " + value);
        return value;
    }

    public int write(double v, byte[] data, int offset) {
        long value = Double.doubleToLongBits(v);
        //System.out.println("write double: " + v);
        return write(value, data, offset);
    }

    public int write(long value, byte[] data, int offset) {
        data[offset] = (byte) (value & 0xff);
        data[offset+1] = (byte) ((value >> 8) & 0xff);
        data[offset+2] = (byte) ((value >> 16) & 0xff);
        data[offset+3] = (byte) ((value >> 24) & 0xff);
        data[offset+4] = (byte) ((value >> 32) & 0xff);
        data[offset+5] = (byte) ((value >> 40) & 0xff);
        data[offset+6] = (byte) ((value >> 48) & 0xff);
        data[offset+7] = (byte) ((value >> 56) & 0xff);
        return offset + 8;
    }

    public int write(float v, byte[] data, int offset) {
        int value = Float.floatToIntBits(v);
        return write(value, data, offset);
    }

    public int write(int value, byte[] data, int offset) {
        data[offset] = (byte) (value & 0xff);
        data[offset+1] = (byte) ((value >> 8) & 0xff);
        data[offset+2] = (byte) ((value >> 16) & 0xff);
        data[offset+3] = (byte) (value >>> 24);
        return offset + 4;
    }

}
