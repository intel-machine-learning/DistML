package com.intel.distml.platform.server;

import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.japi.Creator;
import com.intel.distml.api.DMatrix;
import com.intel.distml.api.PartitionInfo;
import com.intel.distml.api.Model;

import akka.actor.UntypedActor;
import com.intel.distml.transport.DataBusProtocol;
import com.intel.distml.util.KeyCollection;
import com.intel.distml.util.KeyList;
import com.intel.distml.util.Logger;
import com.intel.distml.util.Matrix;

import java.io.Serializable;

public class ParameterServerActor extends UntypedActor {

    public static class Started implements Serializable {
        private static final long serialVersionUID = 1L;

        final public int parameterServerIndex;
        public Started(int parameterServerIndex) {
            this.parameterServerIndex = parameterServerIndex;
        }
    }

    private ActorSelection monitor;
    private Model model;
    private int parameterServerIndex;


    public static Props props(final String monitorPath, final Model model, final int parameterServerIndex) {
        return Props.create(new Creator<ParameterServerActor>() {
            private static final long serialVersionUID = 1L;
            public ParameterServerActor create() throws Exception {
                return new ParameterServerActor(monitorPath, model, parameterServerIndex);
            }
        });
    }

    ParameterServerActor(String monitorPath, Model model, int parameterServerIndex) {
        this.monitor = getContext().actorSelection(monitorPath);
        this.model = model;
        this.parameterServerIndex = parameterServerIndex;

        // Initialize parameters
        for (String matrixName : model.dataMap.keySet()) {
            DMatrix m = model.getMatrix(matrixName);
            if (!m.hasFlag(DMatrix.FLAG_ON_SERVER)) {
                continue;
            }

            PartitionInfo info = m.serverPartitions();
            KeyCollection keys = m.getRowKeys();  // if not partitioned or copied
            if (info != null) {
                if (info.type == PartitionInfo.Type.PARTITIONED) {
                    keys = info.getPartition(parameterServerIndex).keys;
                }
                else if ((info.type == PartitionInfo.Type.EXCLUSIVE) && (info.exclusiveIndex != parameterServerIndex)) {
                    return;
                }
            }

            log("init parameters on server: " + matrixName);
            m.initOnServer(this.parameterServerIndex, keys);
        }

        log("Parameters initialized");
        this.monitor.tell(new Started(this.parameterServerIndex), getSelf());
    }

	@Override
	public void onReceive(Object msg) {
         if (msg instanceof DataBusProtocol.PartialDataRequest) {// Fetch partial parameters
            DataBusProtocol.PartialDataRequest req = (DataBusProtocol.PartialDataRequest)msg;
            Matrix data = model.getMatrix(req.matrixName).localCache;
            KeyCollection rows = req.rows;
            KeyCollection cols = req.cols;

            log("partial data request received: " + req.matrixName + ", " + req.rows + ", " + rows);
             if (rows instanceof KeyList) {
                 log("keylist size = " + rows.size());
             }
            Matrix result = data.subMatrix(rows, cols);

            log("send data: " + result + ", row size = " + result.getRowKeys().size());
            getSender().tell(new DataBusProtocol.Data(req.matrixName, result), getSelf());

        } else if (msg instanceof DataBusProtocol.FetchDataRequest) { // Fetch parameters of one layer
            DataBusProtocol.FetchDataRequest req = (DataBusProtocol.FetchDataRequest)msg;

             Matrix data = model.getMatrix(req.matrixName).localCache;
             log("data request received: " + data.getRowKeys());
            getSender().tell(new DataBusProtocol.Data(req.matrixName, data), getSelf());

        } else if (msg instanceof DataBusProtocol.PushDataRequest) {
            log("update push request received.");
            DataBusProtocol.PushDataRequest req = (DataBusProtocol.PushDataRequest)msg;

            for (DataBusProtocol.Data update : req.dataList) {
                if (req.replace) {
                    log("replacing local cache: " + update.data);
                    model.getMatrix(update.matrixName).setLocalCache(update.data);
                }
                else {
                    log("merge begin");
                    model.mergeUpdate(this.parameterServerIndex, update.matrixName, update.data);
                    log("merge done");
                }
            }
             // Always return successful current now
             getSender().tell(new DataBusProtocol.PushDataResponse(true), getSelf());
        } else unhandled(msg);
	}

    @Override
    public void postStop() {
        log("Parameter server stopped");
        getContext().system().shutdown();
    }

    private void log(String msg) {
        Logger.InfoLog(msg, Logger.Role.PARAMETER_SERVER, this.parameterServerIndex);
    }
}
