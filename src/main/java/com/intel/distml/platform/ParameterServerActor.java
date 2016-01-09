package com.intel.distml.platform;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.io.Tcp;
import akka.io.TcpMessage;
import akka.japi.Creator;
import com.intel.distml.api.DMatrix;
import com.intel.distml.api.Model;

import akka.actor.UntypedActor;
import com.intel.distml.util.*;
import com.intel.distml.util.DoubleArray;
import com.intel.distml.util.IntMatrix;
import com.intel.distml.util.store.DoubleArrayStore;
import com.intel.distml.util.store.DoubleMatrixStore;
import com.intel.distml.util.store.IntArrayStore;
import com.intel.distml.util.store.IntMatrixStore;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

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

    public static class ModelSetup extends DistMLMessage {
        private static final long serialVersionUID = 1L;

        public ModelSetup() {
        }
    }

    public static class ModelSetupDone extends DistMLMessage {
        private static final long serialVersionUID = 1L;

        public ModelSetupDone() {
        }
    }


    private Model model;
    private HashMap<String, DataStore> stores;

    private ActorSelection monitor;
    private int parameterServerIndex;
    private String psNetwordPrefix;

    private int clientCounter;
    private HashMap<Integer, ActorRef> clients = new HashMap<Integer, ActorRef>();

    public static Props props(final Model model, final String monitorPath, final int parameterServerIndex, final String psNetwordPrefix) {
        return Props.create(new Creator<ParameterServerActor>() {
            private static final long serialVersionUID = 1L;
            public ParameterServerActor create() throws Exception {
                return new ParameterServerActor(model, monitorPath, parameterServerIndex, psNetwordPrefix);
            }
        });
    }

    ParameterServerActor(Model model, String monitorPath, int parameterServerIndex, String psNetwordPrefix) {
        this.monitor = getContext().actorSelection(monitorPath);
        this.parameterServerIndex = parameterServerIndex;
        this.model = model;
        this.psNetwordPrefix = psNetwordPrefix;
        this.clientCounter = 0;

        stores = new HashMap<String, DataStore>();
        for (Map.Entry<String, DMatrix> m : model.dataMap.entrySet()) {
            if (m.getKey().equals("SAMPLE")) continue;

            stores.put(m.getKey(), createStore(parameterServerIndex, m.getValue()));
        }

        try {
            final ActorRef tcp = Tcp.get(getContext().system()).manager();
            InetSocketAddress addr = new InetSocketAddress(Utils.getLocalIP(psNetwordPrefix), 0);
            tcp.tell(TcpMessage.bind(getSelf(), addr, 100), getSelf());
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        log("register to monitor");
    }

    public DataStore createStore(int serverIndex, DMatrix matrix) {
        DataDesc format = matrix.getFormat();
        if (format.dataType == DataDesc.DATA_TYPE_ARRAY) {
            if (format.valueType == DataDesc.ELEMENT_TYPE_INT) {
                IntArrayStore store = new IntArrayStore();
                store.init(matrix.partitions[serverIndex]);
                return store;
            }
            else if (format.valueType == DataDesc.ELEMENT_TYPE_DOUBLE) {
                DoubleArrayStore store = new DoubleArrayStore();
                store.init(matrix.partitions[serverIndex]);
                return store;
            }
        }
        else {
            if (format.valueType == DataDesc.ELEMENT_TYPE_INT) {
                IntMatrixStore store = new IntMatrixStore();
                store.init(matrix.partitions[serverIndex], (int) matrix.getColKeys().size());
                return store;
            } else if (format.valueType == DataDesc.ELEMENT_TYPE_DOUBLE) {
                DoubleMatrixStore store = new DoubleMatrixStore();
                store.init(matrix.partitions[serverIndex], (int) matrix.getColKeys().size());
                return store;
            }
        }

        throw new IllegalArgumentException("Unrecognized matrix type: " + matrix.getClass().getName());
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
            clientCounter++;
            ActorRef c = getContext().actorOf(DataRelay.props(clientCounter, getSender(), getSelf()));
            getSender().tell(TcpMessage.register(c), getSelf());
            clients.put(clientCounter, c);
        } else if (msg instanceof DataRelay.Disconnect) {
            log("relay disconnected: " + getSender());
            int id = ((DataRelay.Disconnect)msg).id;
            ActorRef client = clients.get(id);
            clients.remove(id);
            if (client != null) {
                log("stopping: " + getSender() + ", " + client);
                context().stop(client);
//                context().stop(getSender());
            }
        } else if (msg instanceof ModelSetup) {
            monitor.tell(new ModelSetupDone(), getSelf());

        } else if (msg instanceof DataBusProtocol.FetchRequest) {// Fetch parameters
            DataBusProtocol.FetchRequest req = (DataBusProtocol.FetchRequest)msg;

            DMatrix m = model.getMatrix(req.matrixName);
            KeyCollection rows = req.rows;
            KeyCollection cols = req.cols;

            DataStore store = stores.get(req.matrixName);

            log("partial data request received: " + req.matrixName + ", " + req.rows + ", " + rows);

            Object result = store.handleFetch(m.getFormat(), rows);
            log("send back: " + result);
            getSender().tell(result, getSelf());
            result = null;
        } else if (msg instanceof DataBusProtocol.PushRequest) {
            log("update push request received: " + msg);
            DataBusProtocol.PushRequest req = (DataBusProtocol.PushRequest)msg;

            DMatrix m = model.getMatrix(req.matrixName);

            DataStore store = stores.get(req.matrixName);
            store.handlePush(m.getFormat(), req.data);
            getSender().tell(new DataBusProtocol.PushResponse(true), getSelf());
        }

        else unhandled(msg);
    }

    @Override
    public void postStop() {
        log("Parameter server stopped");
        getContext().system().shutdown();
    }

    private void log(String msg) {
        Logger.info(msg, "PS-" + parameterServerIndex);
    }
}
