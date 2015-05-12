package com.intel.distml.platform.monitor;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;

import akka.actor.*;
import akka.japi.Creator;

import akka.pattern.Patterns;
import com.intel.distml.api.Model;
import com.intel.distml.api.ModelWriter;
import com.intel.distml.api.databus.MonitorDataBus;
import com.intel.distml.platform.TrainingConf;
import com.intel.distml.platform.server.ParameterServerActor;
import com.intel.distml.transport.ServerDataBusImpl;
import com.intel.distml.util.*;
import com.intel.distml.platform.worker.WorkerActor;
import com.intel.distml.platform.worker.WorkerLeadActor;
import scala.concurrent.Await;
import scala.concurrent.Future;

import static akka.dispatch.Futures.sequence;

public class MonitorActor extends UntypedActor {

    public static class RegisterResponse implements Serializable {
        private static final long serialVersionUID = 1L;

        final public int workerIndex;     // indicate worker index inside of a group
        final public ActorRef workerLead;  // worker lead
        final public ActorRef[] workers;  // neighbors
        final public ActorRef[] parameterServers;   // parameter servers

        public RegisterResponse(int workerIndex, ActorRef workerLead, ActorRef[] workers, ActorRef[] parameterServers) {
            this.workerIndex = workerIndex;
            this.workerLead = workerLead;
            this.workers = workers;
            this.parameterServers = parameterServers;
        }
    }

    public static class VariableChange implements Serializable {
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

    private ActorRef[] parameterServers;

    private ActorRef[] workers;
    private ActorRef[] workerLeads;
    private int[] workerGroupTrainingProgress;

    private int parameterServerNumber;
    private int workerGroupSize;
    private int workerGroupNum;

    private HashMap<String, Matrix> initialParams;
    private Model model;
    private ModelWriter modelWriter;

    private long totalSamples;
    private long progress;

    /*
     * Dynamic State
     */
    public static enum State {
        CREATED,
        ACTOR_REGISTRATION,
        INIT_PARAMETER_SERVER,
        INIT_WORKER_LEAD,
        MONITOR_TRAINING,
        DONE
        // New states later
    }

    // Worker Lead State
    private class InnerStateData {
        State currentState = State.CREATED;
        int parameterServerRegistrationCount = 0;
        int workerRegistrationCount = 0;
        int workerLeadRegistrationCount = 0;
        int workerGroupInitCount = 0;
        int workerGroupFinishTrainingCount = 0;
    }
    private InnerStateData innerState = new InnerStateData();

    MonitorDataBus dataBus = new MonitorDataBus() {
        public void broadcast(String name, Object value) {
            for (ActorRef worker : workers) {
                worker.tell(new VariableChange(name, value), getSelf());
            }
        }
    };


    Thread pushParameterThread = new Thread() {

        @Override
        public void run() {
            log("push parameter start.");

            ServerDataBusImpl paramDataBus = new ServerDataBusImpl(-1, parameterServers, model, getContext());
            for (String matrixName : initialParams.keySet()) {

                Matrix data = initialParams.get(matrixName);

                KeyRange keys = (KeyRange) data.getRowKeys();
                KeyCollection[] sets = keys.linearSplit(200);
                for (KeyCollection set : sets) {
                    paramDataBus.pushInitialParams(matrixName, data.subMatrix(set, KeyCollection.ALL));
                }
            }

            log("push parameter done.");
            getSelf().tell(new ServerInitDone(), getSelf());
        }
    };

    // Use configuration class later
    public MonitorActor(Model model, HashMap<String, Matrix> params, TrainingConf config, ModelWriter modelWriter) {
        this.parameterServerNumber = config.psCount();
        this.workerGroupSize = config.groupSize();
        this.workerGroupNum = config.groupCount();
        this.parameterServers = new ActorRef[parameterServerNumber];
        this.workers = new ActorRef[workerGroupNum * workerGroupSize];
        this.workerLeads = new ActorRef[workerGroupNum];
        this.workerGroupTrainingProgress = new int[workerGroupNum];

        this.model = model;
        this.initialParams = params;
        this.modelWriter = modelWriter;

        this.totalSamples = config.totalSampleCount();
        this.progress = 0;

        log("Monitor created, parameterServerNumber:" + parameterServerNumber + " workerGroupSize: "
                + workerGroupSize + " workerGroupNum: " + workerGroupNum + " workerLeadNum: " + workerLeads.length);

        // TODO Add parameter server later, set init state to ACTOR_REGISTRATION current now
        setState(State.ACTOR_REGISTRATION);
    }

    public static Props props(final Model model, final HashMap<String, Matrix> params, final TrainingConf config, final ModelWriter modelWriter) {
        return Props.create(new Creator<MonitorActor>() {
            private static final long serialVersionUID = 1L;
            public MonitorActor create() throws Exception {
                return new MonitorActor(model, params, config, modelWriter);
            }
        });
    }

    @Override
    public void onReceive(Object msg) throws Exception {
        if (innerState.currentState == State.ACTOR_REGISTRATION) onReceiveActorRegistration(msg);
        else if (innerState.currentState == State.INIT_PARAMETER_SERVER) onReceiveInitParameterServer(msg);
        else if (innerState.currentState == State.INIT_WORKER_LEAD) onReceiveInitWorkerLead(msg);
        else if (innerState.currentState == State.MONITOR_TRAINING) onReceiveInitMonitorTraining(msg);
        else unhandled(msg);
    }

    @Override
    public void postStop() {
        log("Monitor stopped");
        getContext().system().shutdown();
    }

    /*
	 * Message Handler
	 */
    private void onReceiveActorRegistration(Object msg) {
        if (msg instanceof WorkerLeadActor.RegisterLead) {
            WorkerLeadActor.RegisterLead workerLeadRegisterInfo = (WorkerLeadActor.RegisterLead)msg;
            log("WorkerLead registered: " + workerLeadRegisterInfo.groupIndex);

            // TODO Watch worker lead
            workerLeads[workerLeadRegisterInfo.groupIndex] = getSender();
            ++innerState.workerLeadRegistrationCount;
        } else if (msg instanceof WorkerActor.RegisterRequest) {
            WorkerActor.RegisterRequest workerStartInfo = (WorkerActor.RegisterRequest)msg;
            log("Worker registered: " + workerStartInfo.globalWorkerIndex);

            workers[workerStartInfo.globalWorkerIndex] = getSender();
            ++innerState.workerRegistrationCount;
        } else if (msg instanceof ParameterServerActor.RegisterRequest) {
            ParameterServerActor.RegisterRequest parameterServerStartInfo = (ParameterServerActor.RegisterRequest)msg;
            log("Parameter server registered: " + getSender());

            parameterServers[parameterServerStartInfo.parameterServerIndex] = getSender();
            ++innerState.parameterServerRegistrationCount;
        }
        else unhandled(msg);

        checkAllRegistered();
    }



    private void checkAllRegistered() {
        if ((innerState.parameterServerRegistrationCount == parameterServerNumber)
                && (innerState.workerLeadRegistrationCount == workerGroupNum)
                && (innerState.workerRegistrationCount == workerGroupNum * workerGroupSize)) {

            log("All actors registered");
            setState(State.INIT_PARAMETER_SERVER);
            pushParameterThread.start();
        }
    }

    /*
     * Message Handler
     */
    private void onReceiveInitParameterServer(Object msg) {
        if (msg instanceof ServerInitDone) {
            setState(State.INIT_WORKER_LEAD);

            for (int groupIndex = 0; groupIndex < workerGroupNum; ++groupIndex) {
                int startWorkerIndex = groupIndex*workerGroupSize;
                ActorRef[] workers = Arrays.copyOfRange(this.workers, startWorkerIndex, startWorkerIndex + workerGroupSize);

                log("Send worker group info to worker leads: " + groupIndex);
                workerLeads[groupIndex].tell(new RegisterResponse(0, workerLeads[groupIndex], workers, parameterServers),
                        getSelf());
            }
        }
        else unhandled(msg);

        //checkAllRegistered();
    }

    private void onReceiveInitWorkerLead(Object msg) {
        if (msg instanceof WorkerLeadActor.ReadyInfo) {
            WorkerLeadActor.ReadyInfo readyInfo = (WorkerLeadActor.ReadyInfo)msg;
            log("Worker lead is ready: " + readyInfo.groupIndex);

            // now tell workers "register ok, you can checkin to leader now"
            //for (int groupIndex = 0; groupIndex < workerGroupNum; ++groupIndex) {
                int startWorkerIndex = readyInfo.groupIndex * workerGroupSize;
                ActorRef[] workers = Arrays.copyOfRange(this.workers, startWorkerIndex, startWorkerIndex + workerGroupSize);

                log("Send worker group info to workers: " + readyInfo.groupIndex);
                for (int i = 0; i < workerGroupSize; i++) {
                    RegisterResponse res = new RegisterResponse(i, workerLeads[readyInfo.groupIndex], workers, parameterServers);
                    workers[i].tell(res, getSelf());
                }
            //}

        } else if (msg instanceof WorkerLeadActor.ReadyTrainingInfo) {
            WorkerLeadActor.ReadyTrainingInfo readyInfo = (WorkerLeadActor.ReadyTrainingInfo)msg;
            log("Worker group ready for training: " + readyInfo.groupIndex);

            ++ innerState.workerGroupInitCount;
            if (innerState.workerGroupInitCount == workerGroupNum) {
                log("All worker groups ready");
                setState(State.MONITOR_TRAINING);

                for (int groupIndex = 0; groupIndex < workerGroupNum; ++groupIndex) {
                    log("Tell leader to start training: " + groupIndex);
                    workerLeads[groupIndex].tell(new WorkerLeadActor.TrainingStart(), getSelf());
                }
            }
        } else unhandled(msg);
    }

    private void onReceiveInitMonitorTraining(Object msg) {
        // 1. If worker group down => restart
        // 2. Update training progress
        // 3. If training done, go into DONE state
        if (msg instanceof WorkerLeadActor.ProgressReport) {
            progress += ((WorkerLeadActor.ProgressReport) msg).trained;
            log("progress: " + progress);
            WorkerLeadActor.ProgressReport progressReport = (WorkerLeadActor.ProgressReport)msg;
            workerGroupTrainingProgress[progressReport.groupIndex] = progressReport.totalTrained;

            model.progress(totalSamples, progress, dataBus);

        } else if (msg instanceof WorkerLeadActor.TrainingDone) {
            WorkerLeadActor.TrainingDone trainingDone = (WorkerLeadActor.TrainingDone)msg;
            log("WorkerGroup done: " + getSender());
//            log("WorkerGroup-" + trainingDone.groupIndex + " Training Completed," +
//                    "Totally " + workerGroupTrainingProgress[trainingDone.groupIndex] + " Samples");
            ++innerState.workerGroupFinishTrainingCount;
            if (innerState.workerGroupFinishTrainingCount == workerGroupNum)
                setState(State.DONE);
        } else unhandled(msg);
        // TODO Request interface
    }


    /*
     * State Transition
     */
    private void setState(State newState) {
        stateTransition(innerState.currentState, newState);
    }

    private void stateTransition(State old, State next) {
        if (old == State.MONITOR_TRAINING && next == State.DONE)
            stateTransitionMonitorTrainingToDone();

        log("State Transition from " + old.toString() + " to " + next.toString());
        innerState.currentState = next;
    }

    private void stateTransitionMonitorTrainingToDone() {
        log("Fetching parameters from parameter server");
        if (modelWriter != null) {
            // Use No.(-1) dataBus to mark monitor databus.
            ServerDataBusImpl paramDataBus = new ServerDataBusImpl(-1, parameterServers, model, getContext());
            modelWriter.writeModel(model, paramDataBus);
        }

        log("Start stopping all sub actor systems");
        stopActors(parameterServers);
        log("All parameter servers stopped");

        stopActors(workerLeads);
        log("All worker leads stopped");

        stopActors(workers);
        log("All workers stopped");

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