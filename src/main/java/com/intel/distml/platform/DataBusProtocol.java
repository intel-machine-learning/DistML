package com.intel.distml.platform;

import com.intel.distml.api.Model;
import com.intel.distml.util.DataDesc;
import com.intel.distml.util.KeyCollection;

import java.io.Serializable;
import java.util.HashMap;

/**
 * DataBusProtocol defines all messages between workers and parameters, and inter workers.
 *
 * Created by taotao on 15-2-3.
 */
public class DataBusProtocol {

    public static class ScamlMessage implements Serializable {}

    public static class Data extends ScamlMessage {

        public final DataDesc format;
        public final byte[] data;

        public Data(DataDesc format, byte[] data) {
            this.format = format;
            this.data = data;
        }

        @Override
        public String toString() {
            return "(PushRequest: len=" + data.length + ")";
        }
    }

     public static class FetchRequest extends ScamlMessage {

        final public String matrixName;
        final public KeyCollection rows;
        final public KeyCollection cols;

        public FetchRequest(String matrixName, KeyCollection rows, KeyCollection cols) {
            this.matrixName = matrixName;
            this.rows = rows;
            this.cols = cols;
        }
    }

    public static class PushRequest extends Data {

        public final String matrixName;

        public PushRequest(String matrixName, DataDesc format, byte[] data) {
            super(format, data);
            this.matrixName = matrixName;
        }

        @Override
        public String toString() {
            return "(PushRequest: " + matrixName + ", len=" + data.length + ")";
        }
    }

    public static class PushResponse extends ScamlMessage {
        private static final long serialVersionUID = 1L;

        final public boolean success;
        public PushResponse(boolean success) {
            this.success = success;
        }
    }

}
