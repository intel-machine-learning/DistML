package com.intel.distml.platform;

import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.japi.Creator;
import com.intel.distml.api.Model;

import akka.actor.UntypedActor;
import com.intel.distml.util.*;
import com.intel.distml.util.store.FloatMatrixStoreAdaGrad;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.URI;
import java.util.HashMap;

public class PSActor extends UntypedActor {

    public static int MIN_REPORT_INTERVAL = 1000;

    public static final int OP_LOAD = 0;
    public static final int OP_SAVE = 1;
    public static final int OP_ZERO = 2;
    public static final int OP_RAND = 3;
    public static final int OP_SET  = 4;
    public static final int OP_SET_ALPHA  = 5;

    public static class RegisterRequest implements Serializable {
        private static final long serialVersionUID = 1L;

        final public int index;
        final public String executorId;
        final public String addr;
        final public String hostName;
        final public long freeMemory;
        final long totalMemory;

        public RegisterRequest(int index, String executorId, String hostName, String addr) {
            this.index = index;
            this.executorId = executorId;
            this.addr = addr;
            this.hostName = hostName;
            totalMemory = Runtime.getRuntime().totalMemory();
            freeMemory = Runtime.getRuntime().freeMemory();
        }
    }

    public static class SyncServerInfo implements Serializable {
        private static final long serialVersionUID = 1L;

        final public String addr;
        public SyncServerInfo(String addr) {
            this.addr = addr;
        }

        @Override
        public String toString() {
            return "SyncServerInfo[" + addr + "]";
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

    public static class AgentMessage implements Serializable {
        private static final long serialVersionUID = 1L;

        final public long freeMemory;
        final public long totalMemory;

        public AgentMessage(long freeMemory, long totalMemory) {
            this.freeMemory = freeMemory;
            this.totalMemory = totalMemory;
        }
    }

    public static class Report implements Serializable {
        private static final long serialVersionUID = 1L;

        final public long freeMemory;
        final public long totalMemory;

        public Report(long freeMemory, long totalMemory) {
            this.freeMemory = freeMemory;
            this.totalMemory = totalMemory;
        }
    }

    private Model model;
    private HashMap<String, DataStore> stores;

    private ActorSelection monitor;
    private int serverIndex;
    private String executorId;

    private PSAgent agent;
    private PSSync syncThread;

    private long lastReportTime;

    public static Props props(final Model model, final HashMap<String, DataStore> stores, final String monitorPath,
                              final int parameterServerIndex, final String executorId, final String psNetwordPrefix) {
        return Props.create(new Creator<PSActor>() {
            private static final long serialVersionUID = 1L;
            public PSActor create() throws Exception {
                return new PSActor(model, stores, monitorPath, parameterServerIndex, executorId, psNetwordPrefix);
            }
        });
    }

    PSActor(Model model, HashMap<String, DataStore> stores, String monitorPath, int serverIndex, String executorId, String psNetwordPrefix) {
        this.monitor = getContext().actorSelection(monitorPath);
        this.serverIndex = serverIndex;
        this.executorId = executorId;
        this.model = model;
        this.stores = stores;
        this.lastReportTime = 0;

        agent = new PSAgent(getSelf(), model, stores, psNetwordPrefix);
        agent.start();
        this.monitor.tell(new RegisterRequest(serverIndex, executorId, agent.hostName(), agent.addr()), getSelf());
    }


    @Override
    public void onReceive(Object msg) throws Exception {
        log("onReceive: " + msg);
        if (msg instanceof SyncServerInfo) {
            SyncServerInfo info = (SyncServerInfo) msg;
            String[] s = info.addr.split(":");
            Socket sck = new Socket(s[0], Integer.parseInt(s[1]));
            syncThread = new PSSync(getSelf(), model, stores);
            syncThread.asStandBy(sck);
        }
        else if (msg instanceof ModelSetup) {
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
        else if (msg instanceof AgentMessage) {
            AgentMessage m = (AgentMessage) msg;
            long now = System.currentTimeMillis();
            if ((now - lastReportTime) > MIN_REPORT_INTERVAL) {
                monitor.tell(new Report(m.freeMemory, m.totalMemory), getSelf());
                lastReportTime = now;
            }
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
        if (syncThread != null) {
            syncThread.disconnect();
            try {
                syncThread.join();
            }
            catch (Exception e) {}
        }
        getContext().system().shutdown();
        log("Parameter server stopped");
    }

    private void log(String msg) {
        Logger.info(msg, "PS-" + serverIndex);
    }
}
