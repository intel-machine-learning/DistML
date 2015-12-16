package com.intel.distml.platform;

import com.intel.distml.api.Model;
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

     public static class FetchRawDataRequest extends ScamlMessage {

        final public String matrixName;
        final public KeyCollection rows;
        final public KeyCollection cols;

        public FetchRawDataRequest(String matrixName, KeyCollection rows, KeyCollection cols) {
            this.matrixName = matrixName;
            this.rows = rows;
            this.cols = cols;
        }
    }

    public static class PushUpdateRequest<T> extends ScamlMessage {

        public final String matrixName;
        public final HashMap<Long, T> update;

        public PushUpdateRequest(String matrixName, HashMap<Long, T> update) {
            this.matrixName = matrixName;
            this.update = update;
        }

        @Override
        public String toString() {
            return "(PushUpdateRequest: " + update.size() + ")";
        }
    }
    public static class PushUpdateResponse extends ScamlMessage {
        private static final long serialVersionUID = 1L;

        final public boolean success;
        public PushUpdateResponse(boolean success) {
            this.success = success;
        }
    }

    public static class PushDataResponse extends ScamlMessage {
        private static final long serialVersionUID = 1L;

        final public boolean success;
        public PushDataResponse(boolean success) {
            this.success = success;
        }
    }

}
