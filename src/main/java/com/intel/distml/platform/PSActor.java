package com.intel.distml.platform;

import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.japi.Creator;
import com.intel.distml.api.Model;

import akka.actor.UntypedActor;
import com.intel.distml.util.*;
import com.intel.distml.util.scala.FloatMatrix;
import com.intel.distml.util.store.FloatMatrixStoreAdaGrad;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.HashMap;

public class PSActor extends UntypedActor {

    public static final int OP_LOAD = 0;
    public static final int OP_SAVE = 1;
    public static final int OP_ZERO = 2;
    public static final int OP_RAND = 3;
    public static final int OP_SET  = 4;
    public static final int OP_SET_ALPHA  = 5;

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

        int op;
        String path;
        String value;
        public ModelSetup(int op, String path) {
            this.op = op;
            this.path = path;
            this.value = null;
        }
        public ModelSetup(int op, String path, String value) {
            this.op = op;
            this.path = path;
            this.value = value;
        }
    }

    public static class ModelSetupDone implements Serializable {
        private static final long serialVersionUID = 1L;

        public ModelSetupDone() {
        }
    }

    public static class Stop implements Serializable {
        private static final long serialVersionUID = 1L;

        public Stop() {
        }
    }

    private Model model;
    private HashMap<String, DataStore> stores;

    private ActorSelection monitor;
    private int serverIndex;

    private PSAgent agent;

    public static Props props(final Model model, final HashMap<String, DataStore> stores, final String monitorPath, final int parameterServerIndex, final String psNetwordPrefix) {
        return Props.create(new Creator<PSActor>() {
            private static final long serialVersionUID = 1L;
            public PSActor create() throws Exception {
                return new PSActor(model, stores, monitorPath, parameterServerIndex, psNetwordPrefix);
            }
        });
    }

    PSActor(Model model, HashMap<String, DataStore> stores, String monitorPath, int serverIndex, String psNetwordPrefix) {
        this.monitor = getContext().actorSelection(monitorPath);
        this.serverIndex = serverIndex;
        this.model = model;
        this.stores = stores;

        agent = new PSAgent(model, stores, psNetwordPrefix);
        agent.start();
        this.monitor.tell(new RegisterRequest(serverIndex, agent.addr()), getSelf());
    }


    @Override
    public void onReceive(Object msg) throws Exception {
        log("onReceive: " + msg);
        if (msg instanceof ModelSetup) {
            ModelSetup req = (ModelSetup) msg;
            String path = req.path;
            switch (req.op) {
                case  OP_LOAD:
                    load(path);
                    break;
                case OP_SAVE:
                    save(path);
                    break;
                case OP_RAND:
                    DataStore store = stores.get(path);
                    store.rand();
                    break;
                case OP_ZERO:
                    store = stores.get(path);
                    store.zero();
                    break;
                case OP_SET:
                    store = stores.get(path);
                    store.set(req.value);
                    break;
            }
            monitor.tell(new ModelSetupDone(), getSelf());
        }
        else if (msg instanceof MonitorActor.SetAlpha) {
            MonitorActor.SetAlpha req = (MonitorActor.SetAlpha) msg;
            FloatMatrixStoreAdaGrad store = (FloatMatrixStoreAdaGrad) stores.get(((MonitorActor.SetAlpha) msg).matrixName);
            store.setAlpha(req.initialAlpha, req.minAlpha, req.factor);
            monitor.tell(new ModelSetupDone(), getSelf());
        }
        else if (msg instanceof MonitorActor.IterationDone) {
            agent.closeClients();
            monitor.tell(new ModelSetupDone(), getSelf());
        }
        else if (msg instanceof Stop) {
            agent.disconnect();
            getContext().stop(self());
        }
        else unhandled(msg);
    }

    private void load(String path) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(path), conf);

        for (String name : stores.keySet()) {
            DataStore store = stores.get(name);
            Path dst = new Path(path + "/" + name + "." + serverIndex);
            DataInputStream in = fs.open(dst);

            store.readAll(in);
            in.close();
        }
    }

    private void save(String path) throws IOException {
        log("saving model: " + path);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(path), conf);

        for (String name : stores.keySet()) {
            DataStore store = stores.get(name);
            Path dst = new Path(path + "/" + name + "." + serverIndex);
            DataOutputStream out = fs.create(dst);

            log("saving to: " + dst.getName());
            store.writeAll(out);
            out.flush();
            out.close();
        }
    }

    @Override
    public void postStop() {
        getContext().system().shutdown();
        log("Parameter server stopped");
    }

    private void log(String msg) {
        Logger.info(msg, "PS-" + serverIndex);
    }
}
