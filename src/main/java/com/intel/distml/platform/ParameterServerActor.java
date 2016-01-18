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

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class ParameterServerActor extends UntypedActor {

    public static class RegisterRequest implements Serializable {
        private static final long serialVersionUID = 1L;

        final public int parameterServerIndex;
        final public String addr;

        public RegisterRequest(int parameterServerIndex, String addr) {
            this.parameterServerIndex = parameterServerIndex;
            this.addr = addr;
        }
    }

    public static class ModelSetup implements Serializable {
        private static final long serialVersionUID = 1L;

        public ModelSetup() {
        }
    }

    public static class ModelSetupDone implements Serializable {
        private static final long serialVersionUID = 1L;

        public ModelSetupDone() {
        }
    }

    private Model model;
    private HashMap<String, DataStore> stores;

    private ActorSelection monitor;
    private int parameterServerIndex;
//    private String psNetwordPrefix;

//    private int clientCounter;
//    private HashMap<Integer, ActorRef> clients = new HashMap<Integer, ActorRef>();
//
    private PSAgent agent;

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
//        this.psNetwordPrefix = psNetwordPrefix;
//        this.clientCounter = 0;

        stores = new HashMap<String, DataStore>();
        for (Map.Entry<String, DMatrix> m : model.dataMap.entrySet()) {
            if (m.getKey().equals("SAMPLE")) continue;

            stores.put(m.getKey(), createStore(parameterServerIndex, m.getValue()));
        }

        agent = new PSAgent(model, stores, psNetwordPrefix);
        agent.start();
        this.monitor.tell(new RegisterRequest(parameterServerIndex, agent.addr()), getSelf());
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
        if (msg instanceof ModelSetup) {
            monitor.tell(new ModelSetupDone(), getSelf());
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
