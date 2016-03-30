package com.intel.distml.platform;

import java.io.Serializable;
import java.util.LinkedList;

import akka.actor.*;
import akka.japi.Creator;

import com.intel.distml.api.Model;
import com.intel.distml.util.*;

public class MonitorActor extends UntypedActor {


    public static class DriverRequest implements Serializable {
        private static final long serialVersionUID = 1L;

        boolean done;
        public DriverRequest() {
            done = false;
        }

        public String toString() {
            return "DriverRequest";
        }
    }

    public static class StartTraining extends DriverRequest {
        private static final long serialVersionUID = 1L;

        long size;
        public StartTraining(long size) {
            this.size = size;
        }

        public String toString() {
            return "IterationDone";
        }
    }

    public static class IterationDone extends DriverRequest {
        private static final long serialVersionUID = 1L;

        public IterationDone() {

        }

        public String toString() {
            return "IterationDone";
        }
    }

    public static class LoadModel extends DriverRequest {
        private static final long serialVersionUID = 1L;

        String path;
        public LoadModel(String path) {
            this.path = path;
        }

        public String toString() {
            return "LoadModel";
        }
    }

    public static class SaveModel extends DriverRequest {
        private static final long serialVersionUID = 1L;

        String path;
        public SaveModel(String path) {
            this.path = path;
        }

        public String toString() {
            return "SaveModel";
        }
    }

    public static class ZeroModel extends DriverRequest {
        private static final long serialVersionUID = 1L;

        String matrixName;
        public ZeroModel(String matrixName) {
            this.matrixName = matrixName;
        }

        public String toString() {
            return "ZeroModel";
        }
    }

    public static class RandModel extends DriverRequest {
        private static final long serialVersionUID = 1L;

        String matrixName;
        public RandModel(String matrixName) {
            this.matrixName = matrixName;
        }

        public String toString() {
            return "RandModel";
        }
    }

    public static class SetModel extends DriverRequest {
        private static final long serialVersionUID = 1L;

        String matrixName;
        String value;
        public SetModel(String matrixName, String value) {
            this.matrixName = matrixName;
            this.value = value;
        }

        public String toString() {
            return "SetModel";
        }
    }

    public static class SetAlpha extends DriverRequest {
        private static final long serialVersionUID = 1L;

        String matrixName;
        float initialAlpha;
        float minAlpha;
        float factor;
        public SetAlpha(String matrixName, float initialAlpha, float minAlpha, float factor) {
            this.matrixName = matrixName;
            this.initialAlpha = initialAlpha;
            this.minAlpha = minAlpha;
            this.factor = factor;
        }

        public String toString() {
            return "SetAlpha";
        }
    }

    public static class TrainingDone implements Serializable {
        private static final long serialVersionUID = 1L;

        public TrainingDone() {

        }

        public String toString() {
            return "TrainingDone";
        }
    }

    public static class RegisterResponse implements Serializable {
        private static final long serialVersionUID = 1L;

        final public ActorRef[] parameterServers;   // parameter servers
        final public String[] psAddrs;   // parameter servers address

        public RegisterResponse(ActorRef[] parameterServers, String[] psAddrs) {
            this.parameterServers = parameterServers;
            this.psAddrs = psAddrs;
        }

        public String toString() {
            return "RegisterResponse";
        }
    }

    private final int psCount;

    private ActorRef[] parameterServers;
    private String[] psAddrs;

    private LinkedList<ActorRef> workers;

    private Model model;

    private long trainSetSize;
    private long progress;
    private long lastProgressReport;

    private DriverRequest pendingRequest;
    private int psCounter = 0;


    public MonitorActor(Model model) {
        this.psCount = model.psCount;
        this.parameterServers = new ActorRef[psCount];
        this.psAddrs = new String[psCount];
        this.workers = new LinkedList<ActorRef>();

        this.model = model;

        pendingRequest = null;
        psCounter = 0;

        trainSetSize = 0;
        progress = 0;
        lastProgressReport = 0;

        log("Monitor created, psCount:" + psCount);
    }

    public static Props props(final Model model) {
        return Props.create(new Creator<MonitorActor>() {
            private static final long serialVersionUID = 1L;
            public MonitorActor create() throws Exception {
                return new MonitorActor(model);
            }
        });
    }

    @Override
    public void onReceive(Object msg) throws Exception {
        debug("onReceive: " + msg + ", "  + getSender() );

        if (msg instanceof PSActor.RegisterRequest) {
            PSActor.RegisterRequest req = (PSActor.RegisterRequest) msg;
            log("Parameter server registered: " + getSender());

            parameterServers[req.parameterServerIndex] = getSender();
            psAddrs[req.parameterServerIndex] = req.addr;
            psCounter++;

            log("counter: " + psCounter + ", " + psCount);
            if (psCounter == psCount) {
                model.psReady = true;
                psCounter = 0;
            }
        }
        else if (msg instanceof LoadModel) {
            pendingRequest = (DriverRequest) msg;
            String path = ((LoadModel) msg).path;
            for (int i = 0; i < parameterServers.length; i++) {
                parameterServers[i].tell(new PSActor.ModelSetup(PSActor.OP_LOAD, path), getSelf());
            }
        }
        else if (msg instanceof SaveModel) {
            pendingRequest = (DriverRequest) msg;
            String path = ((SaveModel) msg).path;
            for (int i = 0; i < parameterServers.length; i++) {
                parameterServers[i].tell(new PSActor.ModelSetup(PSActor.OP_SAVE, path), getSelf());
            }
        }
        else if (msg instanceof RandModel) {
            pendingRequest = (DriverRequest) msg;
            String matrixName = ((RandModel) msg).matrixName;
            for (int i = 0; i < parameterServers.length; i++) {
                parameterServers[i].tell(new PSActor.ModelSetup(PSActor.OP_RAND, matrixName), getSelf());
            }
        }
        else if (msg instanceof SetModel) {
            pendingRequest = (DriverRequest) msg;
            String matrixName = ((SetModel) msg).matrixName;
            String value = ((SetModel) msg).value;
            for (int i = 0; i < parameterServers.length; i++) {
                parameterServers[i].tell(new PSActor.ModelSetup(PSActor.OP_ZERO, matrixName, value), getSelf());
            }
        }
        else if (msg instanceof ZeroModel) {
            pendingRequest = (DriverRequest) msg;
            String matrixName = ((ZeroModel) msg).matrixName;
            for (int i = 0; i < parameterServers.length; i++) {
                parameterServers[i].tell(new PSActor.ModelSetup(PSActor.OP_ZERO, matrixName), getSelf());
            }
        }
        else if (msg instanceof SetAlpha) {
            pendingRequest = (DriverRequest) msg;
            for (int i = 0; i < parameterServers.length; i++) {
                parameterServers[i].tell(msg, getSelf());
            }
        }
        else if (msg instanceof IterationDone) {
            pendingRequest = (DriverRequest) msg;
            for (int i = 0; i < parameterServers.length; i++) {
                parameterServers[i].tell(msg, getSelf());
            }

            progress = 0;
            lastProgressReport = 0;
        }
        else if (msg instanceof StartTraining) {
            trainSetSize = ((StartTraining) msg).size;
            log("train set size: " + trainSetSize);
            progress = 0;
            lastProgressReport = 0;

            ((StartTraining) msg).done = true;
        }
        else if (msg instanceof PSActor.ModelSetupDone) {
            psCounter++;
            if (psCounter == psCount) {
                pendingRequest.done = true;
                psCounter = 0;
            }
        }
        else if (msg instanceof WorkerActor.RegisterRequest) {
            WorkerActor.RegisterRequest info = (WorkerActor.RegisterRequest) msg;

            workers.add(getSender());
            getSender().tell(new RegisterResponse(parameterServers, psAddrs), getSelf());
        } else if (msg instanceof WorkerActor.Progress) {
            progress += ((WorkerActor.Progress) msg).sampleCount;
            if (trainSetSize > 0) {
                if (progress == trainSetSize) {
                    log("progress: 100%");
                } else if (((double) progress - lastProgressReport) / trainSetSize > 0.01) {
                    lastProgressReport = progress;
                    log("progress: " + (lastProgressReport * 100 / trainSetSize) + "%");
                }
            }
            else {
                log("progress: " + progress);
            }
        } else if (msg instanceof TrainingDone) {
            stopAll();
        }
    }

    @Override
    public void postStop() {
        log("Monitor stopped");
        getContext().system().shutdown();
    }

    private void stopAll() {
        stopActors(parameterServers);

        getContext().stop(getSelf());
        log("Start stopping monitor");
    }

    private void stopActors(ActorRef[] actors) {
        for (ActorRef ps : parameterServers) {
            ps.tell(new PSActor.Stop(), self());
        }
    }

    private void debug(String msg) {
        Logger.debug(msg, "Monitor");
    }

    private void log(String msg) {
        Logger.info(msg, "Monitor");
    }
}