package com.intel.distml.util.store;

import com.intel.distml.util.*;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

/**
 * Created by yunlong on 1/3/16.
 */
public class FloatMatrixStoreAdaGrad extends DataStore {
    public static final int VALUE_SIZE = 4;
    public static final int ALPHA_SIZE = 4;

    transient KeyCollection localRows;
    transient int rowSize;
    transient float[][] localData;

    transient float initialAlpha, minAlpha;
    transient float[][] alpha;
    transient float[][] delta;

    transient float factor = 1.5f;
    transient float maxDelta = 0f;
    transient int maxDeltaRow = 0;
    transient int maxDeltaCol = 0;

    public KeyCollection rows() {
        return localRows;
    }
    public int rowSize() {
        return rowSize;
    }

    public void init(KeyCollection keys, int cols) {
        this.localRows = keys;
        this.rowSize = cols;
        localData = new float[(int)localRows.size()][rowSize];

        alpha = new float[(int)localRows.size()][rowSize];
        delta = new float[(int)localRows.size()][rowSize];

        for (int i = 0; i < localRows.size(); i++) {
            for (int j = 0; j < rowSize; j++) {
                localData[i][j] = 0.0f;
                alpha[i][j] = 0.0f;
                delta[i][j] = 0.0f;
            }
        }
    }

    public void rand() {
        System.out.println("init with random values");

        int rows = (int) localRows.size();

        Random r = new Random();
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < rowSize; j++) {
                int a = r.nextInt(100);
                localData[i][j] = (a / 100.0f - 0.5f) / rowSize;
            }
        }
    }

    public void set(String value) {
        setValue(Float.parseFloat(value));
    }

    public void zero(String value) {
        setValue(0f);
    }

    public void setAlpha(float initialAlpha, float minAlpha, float factor) {
        setAlphaValue(initialAlpha);
        this.initialAlpha = initialAlpha;
        this.minAlpha = minAlpha;
        this.factor = factor;
    }

    private void setValue(float v) {
        System.out.println("init with value: " + v);

        int rows = (int) localRows.size();

        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < rowSize; j++) {
                localData[i][j] = v;
            }
        }
    }

    private void setAlphaValue(float v) {
        System.out.println("init alpha with value: " + v);

        int rows = (int) localRows.size();

        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < rowSize; j++) {
                alpha[i][j] = v;
            }
        }
    }

    @Override
    public void writeAll(DataOutputStream os) throws IOException {
        for (int i = 0; i < localData.length; i++) {
            for (int j = 0; j < rowSize; j++) {
                os.writeFloat(localData[i][j]);
            }
        }
    }

    @Override
    public void readAll(DataInputStream is) throws IOException {
        for (int i = 0; i < localData.length; i++) {
            for (int j = 0; j < rowSize; j++) {
                localData[i][j] = is.readFloat();
            }
        }
    }

    @Override
    public void syncTo(DataOutputStream os, int fromRow, int toRow) throws IOException {
        for (int i = fromRow; i <= toRow; i++) {
            for (int j = 0; j < rowSize; j++) {
                os.writeFloat(localData[i][j]);
            }
        }
    }

    @Override
    public void syncFrom(DataInputStream is, int fromRow, int toRow) throws IOException {
        int rowSize = (int) localRows.size();
        for (int i = fromRow; i <= toRow; i++) {
            for (int j = 0; j < rowSize; j++) {
                localData[i][j] = is.readFloat();
            }
        }
    }

    @Override
    public byte[] handleFetch(DataDesc format, KeyCollection rows) {

        System.out.println("handle fetch request: " + rows);
        KeyCollection keys = localRows.intersect(rows);
        byte[] buf;
        if (format.denseColumn) {
            int keySpace = (int) (format.keySize * keys.size());
            int valueSpace = (int) ((VALUE_SIZE + ALPHA_SIZE) * keys.size() * localData[0].length);
            buf = new byte[keySpace + valueSpace];
            System.out.println("buf size: " + buf.length);
        }
        else {
            int nzcount = 0;
            Iterator<Long> it = keys.iterator();
            while (it.hasNext()) {
                long k = it.next();
                float[] values = localData[indexOf(k)];
                for (int i = 0; i < values.length; i++) {
                    if (values[i] != 0.0) {
                        nzcount++;
                    }
                }
            }
            int len = (VALUE_SIZE + ALPHA_SIZE + 4) * nzcount;
            buf = new byte[format.keySize * (int)keys.size() + len];
        }

        Iterator<Long> it = keys.iterator();
        int offset = 0;
        while(it.hasNext()) {
            long k = it.next();
            format.writeKey((Number)k, buf, offset);
            offset += format.keySize;

            int index = indexOf(k);
            float[] values = localData[index];
            float[] alphas = alpha[index];
            if (format.denseColumn) {
                for (int i = 0; i < values.length; i++) {
                    format.writeValue(values[i], buf, offset);
                    offset += VALUE_SIZE;
                    format.write(alphas[i], buf, offset);
                    offset += ALPHA_SIZE;
                }
            }
            else {
                int counterIndex = offset;
                offset += 4;

                int counter = 0;
                for (int i = 0; i < values.length; i++) {
                    if (values[i] != 0) {
                        format.write(i, buf, offset);
                        offset += 4;
                        format.write(values[i], buf, offset);
                        offset += VALUE_SIZE;
                        format.write(alphas[i], buf, offset);
                        offset += ALPHA_SIZE;
                    }

                    counter++;
                }
                format.write(counter, buf, counterIndex);
            }
        }

        return buf;
    }

    public int indexOf(long key) {
        if (localRows instanceof KeyRange) {
            return (int) (key - ((KeyRange)localRows).firstKey);
        }
        else if (localRows instanceof KeyHash) {
            KeyHash hash = (KeyHash) localRows;
            return (int) ((key - hash.minKey) % hash.hashQuato);
        }

        throw new RuntimeException("Only KeyRange or KeyHash is allowed in server storage");
    }

    public long keyOf(int index) {
        if (localRows instanceof KeyRange) {
            return ((KeyRange)localRows).firstKey + index;
        }
        else if (localRows instanceof KeyHash) {
            KeyHash hash = (KeyHash) localRows;
            return hash.minKey + index * hash.hashQuato;
        }

        throw new RuntimeException("Only KeyRange or KeyHash is allowed in server storage");
    }

    public void handlePush(DataDesc format, byte[] data) {
        int offset = 0;
        while (offset < data.length) {
            long key = format.readKey(data, offset).longValue();
            offset += format.keySize;
            offset = updateRow(key, data, offset, format);
        }
        System.out.println("max delta: " + maxDeltaRow + ", " + maxDeltaCol + ", " + maxDelta);
    }

    private int updateRow(long key, byte[] data, int start, DataDesc format) {
        assert(localRows.contains(key));

        int index = indexOf(key);
        float[] row = localData[index];
        float[] alphas = alpha[index];
        float[] deltas = delta[index];
        int offset = start;
        if (format.denseColumn) {
//            if (key == 100L) {
//                System.out.println("after updated : row(100)(0) = " + row[0] + ", " + deltas[0] + ", " + alphas[0]);
//            }

            for (int i = 0; i < row.length; i++) {
                float update = format.readFloat(data, offset);

                row[i] += update;
                //adapGrad
                deltas[i] += update * update;
                if (deltas[i] > 1.0) {
                    alphas[i] = (float) (initialAlpha / (factor * Math.sqrt(deltas[i])));
                    if (alphas[i] < minAlpha)
                        alphas[i] = minAlpha;
                }
                if (deltas[i] > maxDelta) {
                    maxDelta = deltas[i];
                    maxDeltaRow = (int)key;
                    maxDeltaCol = i;
                }

                offset += VALUE_SIZE;
            }
//            if (key == 100L) {
//                System.out.println("after updated : row(100)(0) = " + row[0] + ", " + deltas[0] + ", " + alphas[0]);
//            }
        }
        else {
            int count = format.readInt(data, offset);
            offset += 4;
            for (int i = 0; i < count; i++) {
                int col = format.readInt(data, offset);
                offset += 4;
                assert(col < row.length);

                float update = format.readFloat(data, offset);
                row[i] += update;
                deltas[i] += update * update;
                if (deltas[i] > 1.0) {
                    alphas[i] = (float) (initialAlpha / Math.sqrt(deltas[i]));
                    if (alphas[i] <= minAlpha)
                        alphas[i] = minAlpha;
                }
                offset += VALUE_SIZE;
            }
        }

        return offset;
    }

    public Iter iter() {
        return new Iter();
    }

    public class Iter {

        int p;

        public Iter() {
            p = -1;
        }

        public boolean hasNext() {
            return p < localData.length - 1;
        }

        public long key() {
            return keyOf(p);
        }

        public float[] value() {
            System.out.println("values: " + localData[p] + ", " + delta[p] + ", " + alpha[p]);
            return localData[p];
        }

        public boolean next() {
            p++;
            return p < localData.length;
        }
    }
}
