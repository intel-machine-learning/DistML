package com.intel.distml.model.cnn;

import com.intel.distml.util.KeyCollection;
import com.intel.distml.util.KeyRange;
import com.intel.distml.util.Matrix;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Iterator;

/**
 * Created by yunlong on 2/7/15.
 */
public class FullConnectionVector extends Matrix {

    float[] values;
    KeyRange rowKeys;

    public FullConnectionVector(int outputCount) {
        values = new float[outputCount];
        rowKeys = new KeyRange(0, outputCount);
    }

    public float element(long row) {
        if (rowKeys.contains(row)) {
            int r = (int) (row - rowKeys.firstKey);
            return values[r];
        }

        throw new IndexOutOfBoundsException("row=" + row + ", row range=" + rowKeys);
    }

    public KeyCollection getRowKeys() {
        return rowKeys;
    }

    public KeyCollection getColKeys() {
        return KeyRange.Single;
    }

    public void init() {
        
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Code below is for partitioning only. If the layer doesn't need partitioning,
    // the code can be removed.
    ////////////////////////////////////////////////////////////////////////////////

    public Matrix subMatrix(KeyCollection rows, KeyCollection cols) {
        if ((rows instanceof KeyRange) && (cols instanceof KeyRange)) {
            return new SubMatrix((KeyRange)rows, (KeyRange)cols);
        }

        throw new RuntimeException("submatrix for non-KeyRange is not supprted");
    }

    private class SubMatrix extends Matrix {

        KeyRange rows;

        public SubMatrix(KeyRange rows, KeyRange cols) {
            this.rows = rows;
        }

        @Override
        public KeyCollection getRowKeys() {
            return rows;
        }

        @Override
        public KeyCollection getColKeys() {
            return KeyRange.Single;
        }

        private void writeObject(ObjectOutputStream os) throws IOException {
            System.out.println("use customized writer <writeObject>");
            //os.defaultWriteObject();

            os.writeObject(rows);
            Iterator<Long> rowIt = rows.iterator();

            while (rowIt.hasNext()) {
                long r = rowIt.next();
                os.writeFloat(element(r));
            }
        }

        private void readObject(ObjectInputStream is) throws IOException, ClassNotFoundException {
            rows = (KeyRange) is.readObject();

            values = new float[(int)rows.size()];
            for (int i = 0; i < rows.size(); i++) {
                values[i] = is.readFloat();
            }
        }
    }

}
