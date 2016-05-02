package com.intel.distml.platform;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.Properties;

import akka.actor.*;
import akka.japi.Creator;

import com.intel.distml.api.Model;
import com.intel.distml.util.*;

public class MonitorActor extends UntypedActor implements PSManager.PSMonitor{

    public static final int TRAIN_BSP = 0;
    public static final int TRAIN_ASGD = 1;
    public static final int TRAIN_SSP = 2;
    public static final int TRAIN_SSGD = 3;

    public static class DriverRequest implements Serializable {
        private static final long serialVersionUID = 1L;

        public boolean done;
        public DriverRequest() {
            done = false;
        }

        public String toString() {
            return "DriverRequest";
        }
    }

    public static class StartTraining extends DriverRequest {
        private static final long serialVersionUID = 1L;

        int trainType;
        long size;
        int maxIterations;
        int maxLag;

        public StartTraining(long size) {
            this(TRAIN_ASGD, size);
        }

        public StartTraining(int trainType, long size) {
            this.trainType = trainType;
            this.size = size;
        }

        public static StartTraining ssp(int maxIterations, int maxLag) {
            StartTraining req = new StartTraining(TRAIN_SSP, 0);
            req.maxIterations = maxIterations;
            req.maxLag = maxLag;

            return req;
        }

        public String toString() {
            return "IterationDone";
        }
    }

    public static class PSTerminated extends DriverRequest {
        private static final long serialVersionUID = 1L;

        public String executorId;
        public PSTerminated(String executorId) {
            this.executorId = executorId;
        }

        public String toString() {
            return "PSTerminated";
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

        Properties report;
        public TrainingDone() {
            this(new Properties());
        }
        public TrainingDone(Properties report) {
            this.report = report;
        }

        public String toString() {
            return "TrainingDone";
        }
    }

    public static class SSP_IterationDone implements Serializable {
        private static final long serialVersionUID = 1L;

        int workerIndex;
        int iteration;
        double cost;
        public SSP_IterationDone(int workerIndex, int iteration) {
            this(workerIndex, iteration, 0.0);
        }
        public SSP_IterationDone(int workerIndex, int iteration, double cost) {
            this.workerIndex = workerIndex;
            this.iteration = iteration;
            this.cost = cost;
        }

        public String toString() {
            return "SSP_IterationDone";
        }
    }

    public static class SSP_IterationNext implements Serializable {
        private static final long serialVersionUID = 1L;

        public SSP_IterationNext() {
        }

        public String toString() {
            return "SSP_IterationNext";
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

    private static class WorkerInfo {
        int workerIndex;
        ActorRef ref;

        public WorkerInfo(int workerIndex, ActorRef ref) {
            this.workerIndex = workerIndex;
            this.ref = ref;
        }
    }

    private final int psCount;

//    private ActorRef[] parameterServers;
//    private String[] psAddrs;
    PSManager psManager;

    private LinkedList<WorkerInfo> workers;

    private Model model;

    private int trainType;
    private long trainSetSize;
    private long progress;
    private long lastProgressReport;

    private DriverRequest pendingRequest;
    private int psCounter = 0;

    private SSP ssp;


    public MonitorActor(Model model) {
        this.psCount = model.psCount;
//        this.parameterServers = new ActorRef[psCount];
//        this.psAddrs = new String[psCount];
        psManager = new PSManager(this, psCount);
        this.workers = new LinkedList<WorkerInfo>();

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

    public void switchServer(int index, String addr, ActorRef actor) {
        log("notify all users to switch to new server: " + index + ", " + addr);
        WorkerActor.PsAvailable info = new WorkerActor.PsAvailable(index, addr);
        for (WorkerInfo w : workers) {
            w.ref.tell(info, getSelf());
        }
    }

    public void psFail() {
        stopWorkers();
        stopServers();
        stop();
    }


    @Override
    public void onReceive(Object msg) throws Exception {
        debug("onReceive: " + msg + ", "  + getSender() );

        if (msg instanceof PSActor.RegisterRequest) {
            PSActor.RegisterRequest req = (PSActor.RegisterRequest) msg;
            log("Parameter server registered: host=" + req.hostName + ", addr=" + req.addr + ", free=" + req.freeMemory + ", total=" + req.totalMemory);

            getContext().watch(getSender());
            if (psManager.serverAvailable(req.index, req.executorId, req.addr, getSender())) {
                psCounter++;

                log("counter: " + psCounter + ", " + psCount);
                if (psCounter == psCount) {
                    model.psReady = true;
                    psCounter = 0;
                }
            }
            else {
                String addr = psManager.getAddr(req.index);
                log("ps already registered on [" + req.index + "], reply to standby with " + addr);
                getSender().tell(new PSActor.SyncServerInfo(addr), getSelf());
            }
        }
        else if (msg instanceof Terminated) {
            getContext().unwatch(getSender());
            psManager.serverTerminated(getSender());
        }
        else if (msg instanceof PSTerminated) {
            psManager.serverTerminated(((PSTerminated) msg).executorId);
        }
        else if (msg instanceof LoadModel) {
            pendingRequest = (DriverRequest) msg;
            String path = ((LoadModel) msg).path;
            for (int i = 0; i < psCount; i++) {
                psManager.getActor(i).tell(new PSActor.ModelSetup(PSActor.OP_LOAD, path), getSelf());
            }
        }
        else if (msg instanceof SaveModel) {
            pendingRequest = (DriverRequest) msg;
            String path = ((SaveModel) msg).path;
            for (int i = 0; i < psCount; i++) {
                psManager.getActor(i).tell(new PSActor.ModelSetup(PSActor.OP_SAVE, path), getSelf());
            }
        }
        else if (msg instanceof RandModel) {
            pendingRequest = (DriverRequest) msg;
            String matrixName = ((RandModel) msg).matrixName;
            for (int i = 0; i < psCount; i++) {
                psManager.getActor(i).tell(new PSActor.ModelSetup(PSActor.OP_RAND, matrixName), getSelf());
            }
        }
        else if (msg instanceof SetModel) {
            pendingRequest = (DriverRequest) msg;
            String matrixName = ((SetModel) msg).matrixName;
            String value = ((SetModel) msg).value;
            for (int i = 0; i < psCount; i++) {
                psManager.getActor(i).tell(new PSActor.ModelSetup(PSActor.OP_ZERO, matrixName, value), getSelf());
            }
        }
        else if (msg instanceof ZeroModel) {
            pendingRequest = (DriverRequest) msg;
            String matrixName = ((ZeroModel) msg).matrixName;
            for (int i = 0; i < psCount; i++) {
                psManager.getActor(i).tell(new PSActor.ModelSetup(PSActor.OP_ZERO, matrixName), getSelf());
            }
        }
        else if (msg instanceof SetAlpha) {
            pendingRequest = (DriverRequest) msg;
            for (int i = 0; i < psCount; i++) {
                psManager.getActor(i).tell(msg, getSelf());
            }
        }
        else if (msg instanceof IterationDone) {
            pendingRequest = (DriverRequest) msg;
            for (int i = 0; i < psCount; i++) {
                psManager.getActor(i).tell(msg, getSelf());
            }

            progress = 0;
            lastProgressReport = 0;
        }
        else if (msg instanceof StartTraining) {
            StartTraining req = (StartTraining) msg;
            trainType = req.trainType;
            trainSetSize = req.size;
            log("train set size: " + trainSetSize);
            progress = 0;
            lastProgressReport = 0;

            if (trainType == TRAIN_SSP) {
                ssp = new SSP(req.maxIterations, req.maxLag);
            }

            ((StartTraining) msg).done = true;
        }
        else if (msg instanceof PSActor.ModelSetupDone) {
            //PSActor.ModelSetupDone t = (PSActor.ModelSetupDone) msg;

            psCounter++;
            if (psCounter == psCount) {
                pendingRequest.done = true;
                psCounter = 0;
            }
        }
        else if (msg instanceof PSActor.Report) {
            PSActor.Report r = (PSActor.Report) msg;
            log("Parameter server report: free=" + r.freeMemory + ", total=" + r.totalMemory);
        }
        else if (msg instanceof WorkerActor.RegisterRequest) {
            WorkerActor.RegisterRequest info = (WorkerActor.RegisterRequest) msg;

            workers.add(new WorkerInfo(info.workerIndex, getSender()));
            getSender().tell(new RegisterResponse(psManager.getActors(), psManager.getAddrs()), getSelf());
        }
        else if (msg instanceof WorkerActor.Progress) {
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
        }
        else if (msg instanceof SSP_IterationDone) {
            SSP_IterationDone req = (SSP_IterationDone) msg;

            if (Math.abs(req.cost) > 1e-6) {
                log("worker " + req.workerIndex + " progress: " + req.iteration + " cost = " + req.cost);
            }
            else {
                log("worker " + req.workerIndex + " progress: " + req.iteration);
            }
            SSP.CheckResult result = ssp.progress(req.workerIndex, req.iteration);
            if (!result.waiting) {
                log("tell worker " + req.workerIndex + " to contiune");
                getSender().tell(new SSP_IterationNext(), getSelf());
            }

            for (int i : result.workersToNotify) {
                for (WorkerInfo w : workers) {
                    if (w.workerIndex == i) {
                        log("tell worker " + i + " to resume");
                        w.ref.tell(new SSP_IterationNext(), getSelf());
                    }
                }
            }


        } else if (msg instanceof TrainingDone) {
            log("Training complete");
            stopServers();
            stop();
        }
    }

    @Override
    public void postStop() {
        log("Monitor stopped");
        getContext().system().shutdown();
    }

    private void stopWorkers() {
        for (WorkerInfo w : workers) {
            w.ref.tell(new WorkerActor.Command(WorkerActor.CMD_STOP), self());
        }
    }

    private void stopServers() {
        LinkedList<ActorRef> actors =  psManager.getAllActors();
        for (ActorRef ps : actors) {
            ps.tell(new PSActor.Stop(), self());
        }
    }

    private void stop() {
        getContext().stop(getSelf());
        log("Start stopping monitor");
    }

    private void debug(String msg) {
        Logger.debug(msg, "Monitor");
    }

    private void log(String msg) {
        Logger.info(msg, "Monitor");
    }
}