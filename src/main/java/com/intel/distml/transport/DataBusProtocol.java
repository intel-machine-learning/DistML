package com.intel.distml.transport;

import com.intel.distml.util.KeyCollection;
import com.intel.distml.util.Matrix;

import java.io.Serializable;
import java.util.LinkedList;

/**
 * DataBusProtocol defines all messages between workers and parameters, and inter workers.
 *
 * Created by taotao on 15-2-3.
 */
public class DataBusProtocol {

    public static class ScamlMessage implements Serializable {}

    //=========================================================================
    // General Messages
    //=========================================================================

    public static class SampleRequest extends ScamlMessage {

        SampleRequest() {
        }
    }

    // fetch all data of specified matrix
    public static class FetchDataRequest extends ScamlMessage {

        final public String matrixName;

        FetchDataRequest(String matrixName) {
            this.matrixName = matrixName;
        }
    }

    // fetch part of specified matrix
    public static class PartialDataRequest extends FetchDataRequest {

        final public KeyCollection rows;
        final public KeyCollection cols;

        PartialDataRequest(String matrixName, KeyCollection rows, KeyCollection cols) {
            super(matrixName);
            this.rows = rows;
            this.cols = cols;
        }
    }

    // data list returned per request
    public static class DataList extends ScamlMessage {
        private static final long serialVersionUID = 1L;

        public final LinkedList<Matrix> dataList;
        public DataList (LinkedList<Matrix> dataList) {
            this.dataList = dataList;
        }
    }

    // data returned per request
    public static class Data extends ScamlMessage {
        private static final long serialVersionUID = 1L;

        public final String matrixName;
        public final Matrix data;
        public Data(String matrixName, Matrix _data) {
            this.matrixName = matrixName;
            this.data = _data;
        }
    }

    // data returned per request
    public static class InitializeData extends ScamlMessage {
        private static final long serialVersionUID = 1L;

        public final String matrixName;
        public final Matrix data;
        public InitializeData(String matrixName, Matrix _data) {
            this.matrixName = matrixName;
            this.data = _data;
        }
    }

    //=========================================================================
    // Messages between workers
    //=========================================================================

    // fetch input data before training
    public static class PushDataRequest extends ScamlMessage {

        public final LinkedList<Data> dataList;
        public final boolean replace;

        public PushDataRequest(LinkedList<Data> dataList) {
            this.dataList = dataList;
            replace = false;
        }

        public PushDataRequest(String matrixName, boolean replace, Matrix _data) {
            dataList = new LinkedList<Data>();
            dataList.add(new Data(matrixName, _data));
            this.replace = replace;
        }

        @Override
        public String toString() {
            return "(PushData: " + dataList.get(0) + ")";
        }
    }

    public static class PushDataResponse extends ScamlMessage {
        private static final long serialVersionUID = 1L;

        final public boolean success;
        public PushDataResponse(boolean success) {
            this.success = success;
        }
    }

/*
    // Only used by worker
    public static class WorkerDataRequest implements Serializable {
        private static final long serialVersionUID = 1L;

        final public int workerIndex;
        WorkerDataRequest(int workerIndex) {
            this.workerIndex = workerIndex;
        }
    }
*/

}
