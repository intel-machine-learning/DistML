package com.intel.distml.platform;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.io.Tcp;
import akka.io.TcpMessage;
import akka.japi.Creator;
import akka.util.ByteString;
import akka.util.ByteStringBuilder;
import com.intel.distml.api.DMatrix;
import com.intel.distml.api.PartitionInfo;
import com.intel.distml.api.Model;

import akka.actor.UntypedActor;
import com.intel.distml.util.*;
import org.apache.spark.broadcast.Broadcast;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteOrder;
import java.util.LinkedList;

public class ParameterServerActor extends UntypedActor {

    public static class RegisterRequest extends DistMLMessage {
        private static final long serialVersionUID = 1L;

        final public int parameterServerIndex;
        final public InetSocketAddress addr;

        public RegisterRequest(int parameterServerIndex, InetSocketAddress addr) {
            this.parameterServerIndex = parameterServerIndex;
            this.addr = addr;
        }
    }

    // request to setup model on server
    public static class ModelSetup extends DistMLMessage {
        private static final long serialVersionUID = 1L;

        public final String appId;
        //public final Model model;
        public ModelSetup(String appId/*, Model model*/) {
            this.appId = appId;
            //this.model = model;
        }
    }

    // request to setup model on server
    public static class ModelSetupDone extends DistMLMessage {
        private static final long serialVersionUID = 1L;

        public final String appId;
        public ModelSetupDone(String appId) {
            this.appId = appId;
        }
    }

    private ActorSelection monitor;
    private Model model;
    private int parameterServerIndex;

    private LinkedList<ActorRef> clients;

    public static Props props(final String monitorPath, final Broadcast<Model> modelBroadcast, final int parameterServerIndex) {
        return Props.create(new Creator<ParameterServerActor>() {
            private static final long serialVersionUID = 1L;
            public ParameterServerActor create() throws Exception {
                return new ParameterServerActor(monitorPath, modelBroadcast, parameterServerIndex);
            }
        });
    }

    ParameterServerActor(String monitorPath, final Broadcast<Model> modelBroadcast, int parameterServerIndex) {
        this.monitor = getContext().actorSelection(monitorPath);
        this.parameterServerIndex = parameterServerIndex;
        this.model = modelBroadcast.getValue();
        this.clients = new LinkedList<ActorRef>();

        try {
            final ActorRef tcp = Tcp.get(getContext().system()).manager();
            InetSocketAddress addr = new InetSocketAddress(Utils.getLocalIP(), 0);
            tcp.tell(TcpMessage.bind(getSelf(), addr, 100), getSelf());
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        log("register to monitor");
    }

    @Override
    public void onReceive(Object msg) throws Exception {
        log("onReceive: " + msg);
        if (msg instanceof Tcp.Bound) {
            Tcp.Bound b = (Tcp.Bound)msg;
            log("Server bound: " + b.localAddress());
            InetSocketAddress addr = b.localAddress();

            this.monitor.tell(new RegisterRequest(this.parameterServerIndex, addr), getSelf());
        } else if (msg instanceof Tcp.CommandFailed) {
            log("error: Server command failed");

        } else if (msg instanceof Tcp.Connected) {
            log("connected: " + getSender());
//            ActorRef c = getContext().actorOf(connectionProps(getSender(), model, parameterServerIndex));
            ActorRef c = getContext().actorOf(DataRelay.props(getSender(), getSelf()));
            getSender().tell(TcpMessage.register(c), getSelf());
            clients.add(c);

        } else if (msg instanceof ModelSetup) {

            ModelSetup req = (ModelSetup)msg;
            initModel();
            getSender().tell(new ModelSetupDone(req.appId), getSelf());

        } else if (msg instanceof DataBusProtocol.PartialDataRequest) {// Fetch partial parameters
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
            //getSender().tell(, getSelf());
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
                if (req.initializeOnly) {
                    log("replacing local cache: " + update.data);
                    DMatrix m = model.getMatrix(update.matrixName);
                    if (m.localCache == null) {
                        m.setLocalCache(update.data);
                    }
                    else {
                        m.mergeMatrix(update.data);
                    }
                }
                else {
                    log("merge begin");
                    model.mergeUpdate(parameterServerIndex, update.matrixName, update.data);
                    log("merge done");
                }
            }
            // Always return successful current now
            getSender().tell(new DataBusProtocol.PushDataResponse(true), getSelf());
        } else unhandled(msg);
    }

    private void initModel() {
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
                } else if ((info.type == PartitionInfo.Type.EXCLUSIVE) && (info.exclusiveIndex != parameterServerIndex)) {
                    return;
                }
            }

            log("init parameters on server: " + matrixName);
            m.initOnServer(this.parameterServerIndex, keys);
        }
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
