package com.intel.distml.platform;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.LinkedList;

import akka.actor.*;
import akka.io.Inet;
import akka.io.Tcp;
import akka.io.TcpMessage;
import akka.japi.Creator;

import akka.pattern.Patterns;
import com.intel.distml.api.Model;
import com.intel.distml.api.ModelWriter;
import com.intel.distml.api.databus.MonitorDataBus;
import com.intel.distml.util.*;
import scala.concurrent.Await;
import scala.concurrent.Future;

import static akka.dispatch.Futures.sequence;

public class MonitorActor extends UntypedActor {

    public static class WorkerStarterRegister extends DistMLMessage {
        private static final long serialVersionUID = 1L;

        public WorkerStarterRegister() {

        }

        public String toString() {
            return "WorkerStarterRegister";
        }
    }

    public static class ParameterServersReady extends DistMLMessage {
        private static final long serialVersionUID = 1L;

        final public ActorRef[] parameterServers;   // parameter servers

        public ParameterServersReady(ActorRef[] parameterServers) {
            this.parameterServers = parameterServers;
        }

        public String toString() {
            return "ParameterServersReady";
        }
    }

    public static class IterationDone extends DistMLMessage {
        private static final long serialVersionUID = 1L;

        final public int iter;
        public IterationDone(final int iter) {
            this.iter = iter;
        }

        public String toString() {
            return "IterationDone";
        }
    }

    public static class IterationDoneAck extends DistMLMessage {
        private static final long serialVersionUID = 1L;

        final public int psIndex;
        public IterationDoneAck(final int psIndex) {
            this.psIndex = psIndex;
        }

        public String toString() {
            return "IterationDoneAck";
        }
    }

    public static class WorkerIterationDone extends DistMLMessage {
        private static final long serialVersionUID = 1L;

        final public int iter;
        final public int workerIndex;
        public WorkerIterationDone(final int workerIndex, final int iter) {
            this.iter = iter;
            this.workerIndex = workerIndex;
        }

        public String toString() {
            return "WorkerIterationDone";
        }
    }

    public static class TrainingDone extends DistMLMessage {
        private static final long serialVersionUID = 1L;

        public TrainingDone() {

        }

        public String toString() {
            return "TrainingDone";
        }
    }

    public static class Stop extends DistMLMessage {
        private static final long serialVersionUID = 1L;

        public long time;
        public Stop() {
            this.time = System.currentTimeMillis();
        }

        public String toString() {
            return "Stop";
        }
    }

    public static class RegisterResponse extends DistMLMessage {
        private static final long serialVersionUID = 1L;

        final public ActorRef[] parameterServers;   // parameter servers
        final public InetSocketAddress[] psAddrs;   // parameter servers address

        public RegisterResponse(ActorRef[] parameterServers, InetSocketAddress[] psAddrs) {
            this.parameterServers = parameterServers;
            this.psAddrs = psAddrs;
        }

        public String toString() {
            return "RegisterResponse";
        }
    }

    public static class VariableChange extends DistMLMessage {
        private static final long serialVersionUID = 1L;

        final public String name;
        final public Object value;

        public VariableChange(String name, Object value) {
            this.name = name;
            this.value = value;
        }
    }

    public static class ServerInitDone implements Serializable {
        private static final long serialVersionUID = 1L;

        public ServerInitDone() {
        }
    }

    private String appId;
    private int psCount;
//    private int workerCount;
    private long totalSamples;
    private long progress;

    //private ActorRef modelSender;
    //private InetSocketAddress addr;

    private ActorRef[] parameterServers;
    private InetSocketAddress[] psAddrs;
    private ActorRef[] serverDataBuses;

    private LinkedList<ActorRef> workers;

//    private int[] trainingProgress;

    private Model model;
    private ModelWriter modelWriter;
    private ActorRef workerStarter;


    /*
     * Dynamic State
     */
    public static enum State {
        CREATED,
        PS_REGISTRATION,
        INIT_PARAMETER_SERVER,
        MONITOR_TRAINING,
        DONE
        // New states later
    }

    // Worker Lead State
    private class InnerStateData {
        State currentState = State.CREATED;
        int psCounter = 0;
//        int workerCounter = 0;
    }
    private InnerStateData innerState = new InnerStateData();

    MonitorDataBus dataBus = new MonitorDataBus() {
        public void broadcast(String name, Object value) {
            for (ActorRef worker : workers) {
                if (worker != null) {
                    worker.tell(new VariableChange(name, value), getSelf());
                }
            }
        }
    };


    // Use configuration class later
    public MonitorActor(String appId, Model model, TrainingContext context, ModelWriter modelWriter) {
        this.appId = appId;
        this.psCount = context.psCount;
//        this.workerCount = context.workerCount;
        this.parameterServers = new ActorRef[psCount];
        this.psAddrs = new InetSocketAddress[psCount];
        this.workers = new LinkedList<ActorRef>();
//        this.trainingProgress = new int[workerCount];

        this.model = model;
        this.modelWriter = modelWriter;

        this.totalSamples = context.totalSampleCount;
        this.progress = 0;

        setState(State.CREATED);
        innerState.psCounter = 0;
//        innerState.workerCounter = 0;

        log("Monitor created, psCount:" + psCount);
        //this.modelSender = getContext().actorOf(ModelSender.props(getSelf(), model));

    }

    public static Props props(final String appId, final Model model, final TrainingContext config, final ModelWriter modelWriter) {
        return Props.create(new Creator<MonitorActor>() {
            private static final long serialVersionUID = 1L;
            public MonitorActor create() throws Exception {
                return new MonitorActor(appId, model, config, modelWriter);
            }
        });
    }

    @Override
    public void onReceive(Object msg) throws Exception {
        log("onReceive: " + msg + ", " + innerState.currentState + ", "
                + innerState.psCounter + ", "
                + getSender() );
        if (innerState.currentState == State.CREATED) {
            if (msg instanceof WorkerStarterRegister) {
                this.workerStarter = getSender();
            }

            if (workerStarter != null) {
                setState(State.PS_REGISTRATION);
            }
        } else if (innerState.currentState == State.PS_REGISTRATION) {

            if (msg instanceof ParameterServerActor.RegisterRequest) {
                ParameterServerActor.RegisterRequest req = (ParameterServerActor.RegisterRequest)msg;
                log("Parameter server registered: " + getSender());

                parameterServers[req.parameterServerIndex] = getSender();
                psAddrs[req.parameterServerIndex] = req.addr;
                ++innerState.psCounter;

                if (innerState.psCounter == psCount) {
                    setState(State.INIT_PARAMETER_SERVER);
                    for (int i = 0; i < parameterServers.length; i++) {
                        parameterServers[i].tell(new ParameterServerActor.ModelSetup(), getSelf());
                    }
                }
            }
        }
        else if (innerState.currentState == State.INIT_PARAMETER_SERVER) {
            if (msg instanceof ParameterServerActor.ModelSetupDone) {
                innerState.psCounter++;
                if (innerState.psCounter == psCount) {
                    setState(State.MONITOR_TRAINING);
                    workerStarter.tell(new ParameterServersReady(parameterServers), getSelf());
                }
            } else unhandled(msg);
        }
        else if (innerState.currentState == State.MONITOR_TRAINING) {
            if (msg instanceof WorkerActor.RegisterRequest) {
                WorkerActor.RegisterRequest info = (WorkerActor.RegisterRequest) msg;
                //log("worker registered: " + info.globalWorkerIndex);

                workers.add(getSender());
                //workers[info.globalWorkerIndex] = getSender();
                getSender().tell(new RegisterResponse(parameterServers, psAddrs), getSelf());
            }
            else if (msg instanceof WorkerActor.ProgressReport) {
                int p = ((WorkerActor.ProgressReport) msg).trained;
                progress += p;
                log("progress: " + p + "[" +progress + ", " + totalSamples + "] from " + getSender());
                //WorkerActor.ProgressReport report = (WorkerActor.ProgressReport)msg;
                //trainingProgress[report.workerIndex] = report.totalTrained;

                model.progress(totalSamples, progress, dataBus);
            } else if (msg instanceof IterationDone) {
                innerState.psCounter = 0;
                workers.clear();
                progress = 0;

                for (ActorRef ps : parameterServers) {
                    ps.tell(msg, getSelf());
                }
            } else if (msg instanceof IterationDoneAck) {
                innerState.psCounter++;
                if (innerState.psCounter == psCount) {
                    innerState.psCounter = 0;

                    //log("Notify workers to start again");
                    workerStarter.tell(msg, getSelf());
/*
                    Stop req = new Stop();
                    log("Notify workers to stop: " + req.time);
                    for (ActorRef w : workers) {
                        w.tell(req, getSelf());
                    }
//                    innerState.workerCounter = 0;
*/
                    innerState.psCounter = 0;
                }

            } else if (msg instanceof TrainingDone) {
                setState(State.DONE);
                saveModel();
            }

            else unhandled(msg);
        }
        else if (innerState.currentState == State.DONE) {
            if (msg instanceof Tcp.Connected) {
                innerState.psCounter++;
                if (innerState.psCounter == psCount) {
                    ServerDataBusImpl paramDataBus = new ServerDataBusImpl(serverDataBuses, model, getContext());
                    modelWriter.writeModel(model, paramDataBus);
                    stopAll();
                }
            }
        }
    }

    @Override
    public void postStop() {
        log("Monitor stopped");
        getContext().system().shutdown();
    }

    /*
     * State Transition
     */
    private void setState(State newState) {
        innerState.currentState = newState;
        if (newState == State.INIT_PARAMETER_SERVER) {
            innerState.psCounter = 0;
        }
//        else if (newState == State.MONITOR_TRAINING) {
//            innerState.workerCounter = 0;
//        }
    }

    private void saveModel() {
        log("Fetching parameters from parameter server");
        if (modelWriter != null) {
            innerState.psCounter = 0;
            final ActorRef tcpManager = Tcp.get(getContext().system()).manager();

            serverDataBuses = new ActorRef[psCount];
            for (int i = 0; i < psCount; i++) {
                serverDataBuses[i] = getContext().actorOf(DataRelay.props(getSelf()));
                tcpManager.tell(TcpMessage.connect(psAddrs[i]), serverDataBuses[i]);
            }
        }
        else {
            stopAll();
        }
    }

    private void stopAll() {
        log("Start stopping all sub actor systems");
        stopActors(parameterServers);
        log("All parameter servers stopped");

//        stopActors(workers);
//        log("All workers stopped");

        getContext().stop(getSelf());
        log("Start stopping monitor");
    }

    private void stopActors(ActorRef[] actors) {
        LinkedList<Future<Boolean>> stopFutures = new LinkedList<Future<Boolean>>();
        Future<Iterable<Boolean>> stopFuture;
        for (ActorRef actor : actors)
            stopFutures.add(Patterns.gracefulStop(actor, Constants.STOP_FUTURE_TIMEOUT_DURATION));
        stopFuture = sequence(stopFutures, getContext().dispatcher());
        try {
            // Block here to wait for termination
            Await.result(stopFuture, Constants.STOP_FUTURE_TIMEOUT_DURATION);
        } catch (Exception e) { // Timeout
            Logger.ErrorLog("******************** Timeout when stopping actors ********************",
                    Logger.Role.MONITOR, 0);
        }
    }

    private void log(String msg) {
        Logger.InfoLog("******************** " + msg + " ********************", Logger.Role.MONITOR, 0);
    }
}