package com.intel.distml.platform.worker;

import java.io.Serializable;
import java.util.LinkedList;

import akka.actor.*;
import akka.dispatch.OnSuccess;
import akka.japi.Creator;

import akka.pattern.Patterns;
import com.intel.distml.api.Model;
import com.intel.distml.platform.TrainingConf;
import com.intel.distml.platform.monitor.MonitorActor;
import com.intel.distml.util.Constants;
import com.intel.distml.util.Logger;
import scala.concurrent.Await;
import scala.concurrent.Future;

import static akka.dispatch.Futures.sequence;

/**
 * Created by yunlong on 12/13/14.
 */
public class WorkerLeadActor extends UntypedActor {

	/*
	 * Messages
	 */
    // Monitor controlling message
    public static class RegisterLead implements Serializable {
        private static final long serialVersionUID = 1L;

        final public int groupIndex;
        public RegisterLead(int groupIndex) {
            this.groupIndex = groupIndex;
        }
    }

    public static class ReadyInfo implements Serializable {
        private static final long serialVersionUID = 1L;

        final public int groupIndex;
        public ReadyInfo(int groupIndex) {
            this.groupIndex = groupIndex;
        }
    }

	public static class CheckInRequest implements Serializable {
        private static final long serialVersionUID = 1L;

        final public int workerIndex;
        final public ActorRef worker;
        final public ActorRef dataBus;
        public CheckInRequest(int workerIndex, ActorRef worker, ActorRef dataBus) {
            this.workerIndex = workerIndex;
            this.worker = worker;
            this.dataBus = dataBus;
        }
	}

    public static class CheckInResponse implements Serializable {
        private static final long serialVersionUID = 1L;

        public final ActorRef[] dataBuses;
        public CheckInResponse(ActorRef[] dataBuses) {
            this.dataBuses = dataBuses;
        }
    }

    public static class ReadyTrainingInfo implements Serializable {
        private static final long serialVersionUID = 1L;

        final public int groupIndex;
        public ReadyTrainingInfo(int groupIndex) {
            this.groupIndex = groupIndex;
        }
    }

    public static class ProgressReport implements Serializable {
        private static final long serialVersionUID = 1L;

        final public int trained;
        final public int totalTrained;
        final public int groupIndex;
        public ProgressReport(int trained, int totalTrained, int groupIndex) {
            this.trained = trained;
            this.totalTrained = totalTrained;
            this.groupIndex = groupIndex;
        }
    }

    // Training controll message
    public static class TrainingStart implements Serializable {
        private static final long serialVersionUID = 1L;

        public final boolean runToComplete;

        public TrainingStart(){
            this(false);
        }

        public TrainingStart(boolean runToComplete){
            this.runToComplete = runToComplete;
        }
    }

    public static class TrainingDone implements Serializable {
        private static final long serialVersionUID = 1L;

        final public int groupIndex;
        public TrainingDone(int groupIndex) {
            this.groupIndex = groupIndex;
        }
    }

    public static class WorkerTrainingDone implements Serializable {
        private static final long serialVersionUID = 1L;

        final public int workerIndex;
        public WorkerTrainingDone(int workerIndex) {
            this.workerIndex = workerIndex;
        }
    }


    static class PreTraining implements Serializable {
        private static final long serialVersionUID = 1L;
        PreTraining(){
            // Add required property later
        }
    }

    static class PostTraining implements Serializable {
        private static final long serialVersionUID = 1L;
        PostTraining(){
            // Add required property later
        }
    }

	static class InputSetup implements Serializable {
		private static final long serialVersionUID = 1L;

		final public int workerIndex;
        final public int sampleNumber;
        //final public int currentProgress;
		InputSetup(int workerIndex, int sampleNumber){
			this.workerIndex = workerIndex;
            this.sampleNumber = sampleNumber;
		}
	}

    // Training controll message
    public static class SampleStart implements Serializable {
        private static final long serialVersionUID = 1L;

        public SampleStart(){
        }
    }

    public static class SampleDone implements Serializable {
        private static final long serialVersionUID = 1L;

        final public int workerIndex;
        final public boolean hasNextSample;         // needed if current worker is sample source
        public SampleDone(int workerIndex, boolean hasNextSample) {
            this.workerIndex = workerIndex;
            this.hasNextSample = hasNextSample;
        }
    }

    static class Barrier implements Serializable {
        private static final long serialVersionUID = 1L;

        final public int workerIndex;
        Barrier(int workerIndex) {
            this.workerIndex = workerIndex;
        }
    }

    static class BarrierOver implements Serializable {
        private static final long serialVersionUID = 1L;

        BarrierOver(){
        }
    }

//	static class ForwardCompute implements Serializable {
//		private static final long serialVersionUID = 1L;
//
//		final public int layerIndex;
//		ForwardCompute(int layerIndex){
//			this.layerIndex = layerIndex;
//		}
//	}
//
//	static class BackwardCompute implements Serializable {
//		private static final long serialVersionUID = 1L;
//
//		final public int layerIndex;
//		BackwardCompute(int layerIndex){
//			this.layerIndex = layerIndex;
//		}
//	}

    private static class RequestComplete implements Serializable {
        private static final long serialVersionUID = 1L;

        final public int completeNumber;
        final public int successNumber;
        RequestComplete(int completeNumber, int successNumber){
            this.completeNumber = completeNumber;
            this.successNumber = successNumber;
        }
    }
	
	/*
	 * Static State
	 */
	private int groupIndex;
    private ActorSelection monitor;
    private ActorRef[] parameterServers;
	private ActorRef[] workers;

    private TrainingConf conf;
	private Model model;

    private LinkedList<ActorRef> barrierRequesters;

	/*
	 * Dynamic State
	 */
	public static enum State {
        CREATED,
        REGISTERED,
        TRAINING,
        POST_TRAINING,
		DONE
		// New states later
	}

	// Worker Lead State
	class InnerStateData {
		State currentState = State.CREATED;

        ActorRef[] dataBuses;

        int workerCount = 0;

		int currentWorkerAsInput = 0;

        int currentTrainingProgress = 0;
        int lastReportProgress = 0;
	}
	private InnerStateData innerState = new InnerStateData();

    WorkerLeadActor(String monitorPath, Model model, int groupIndex, TrainingConf conf) {
        this.model = model;
        this.monitor = getContext().actorSelection(monitorPath);
        this.groupIndex = groupIndex;
        this.conf = conf;

        // Register on monitor
        setState(State.CREATED);
        monitor.tell(new RegisterLead(groupIndex), getSelf());

        barrierRequesters = new LinkedList<ActorRef>();
    }

    public static Props props(final String monitorPath, final Model model,
                              final int groupIndex, final int miniBatchSize, final TrainingConf conf) {
        return Props.create(new Creator<WorkerLeadActor>() {
            private static final long serialVersionUID = 1L;
            public WorkerLeadActor create() throws Exception {
                return new WorkerLeadActor(monitorPath, model, groupIndex, conf);
            }
        });
    }
	
	@Override
	public void onReceive(Object msg) throws Exception {
        log("onReceive: " + msg + ", " + innerState.currentState);
        if (innerState.currentState == State.CREATED) onReceiveAfterCreated(msg);
        else if (innerState.currentState == State.REGISTERED) onReceiveAfterRegistered(msg);
		else if (innerState.currentState == State.TRAINING) onReceiveTraining(msg);
        else if (innerState.currentState == State.POST_TRAINING) onReceivePostTraining(msg);
//        else if (innerState.currentState == State.DONE) onReceiveDone(msg);
        else unhandled(msg);
	}

    @Override
    public void postStop() {
        log("Worker lead stopped");
        // The actor system is stopped by the worker not the worker lead,
        // because worker lead only lays on one of the worker nodes in the
        // worker group.
    }
	
	/*
	 * Message Echo_server
	 */
    private void onReceiveAfterCreated(Object msg) {
        if (msg instanceof MonitorActor.RegisterResponse) {
            MonitorActor.RegisterResponse res = (MonitorActor.RegisterResponse) msg;

            this.parameterServers = res.parameterServers;
            this.workers = res.workers;

            setState(State.REGISTERED);
            getSender().tell(new ReadyInfo(groupIndex), getSelf());
        } else unhandled(msg);
    }

	private void onReceiveAfterRegistered(Object msg) {
        if (msg instanceof CheckInRequest) {
            // Collect dataBus info
            CheckInRequest req = (CheckInRequest)msg;
            innerState.dataBuses[req.workerIndex] = req.dataBus;
            ++innerState.workerCount;
            if (innerState.workerCount == conf.groupSize()) {
                log("All Workers have been initialized");

                // Tell worker to initialize siblings of dataBus
                if (broadcastAndWait(new CheckInResponse(innerState.dataBuses))) {
                    this.monitor.tell(new ReadyTrainingInfo(groupIndex), getSelf());
                }
            }
//        } else if (msg instanceof RequestComplete) {
//            log("All DataBuses initialized with siblings");
//            assert ((RequestComplete)msg).successNumber == workerGroupSize;
//
        } else if (msg instanceof TrainingStart) {
            setState(State.TRAINING);
            barrierRequesters.clear();

            broadcastAndWait(new PreTraining());

            log("broadcast workers to start training.");
            if (conf.groupSize() == 1) {
                innerState.workerCount = 0;
                broadcast(new TrainingStart(true));
            }
            else {
                broadcastAndWait(new TrainingStart(false));
                trainWithNextSample();
            }
            log("start new iteration now.");
        } else unhandled(msg);
	}

	private void onReceiveTraining(Object msg) {
        if (msg instanceof WorkerLeadActor.WorkerTrainingDone) {
            innerState.workerCount++;
            log("worker training done: " + innerState.workerCount);
            if (innerState.workerCount == conf.groupSize()) {
                // todo start new iteration
                stopTraining();
            }
        } else if (msg instanceof Barrier) {
            barrierRequesters.add(getSender());
            if (barrierRequesters.size() == conf.groupSize()) {
                log("barrier over.");
                Object res = new BarrierOver();
                for (ActorRef requester : barrierRequesters) {
                    requester.tell(res, getSelf());
                }
                barrierRequesters.clear();
            }
        } else if (msg instanceof SampleDone) {

            trainWithNextSample();

        } else if (msg instanceof ProgressReport) {
            ProgressReport rep = (ProgressReport) msg;
            monitor.tell(new ProgressReport(rep.trained, rep.totalTrained, groupIndex), getSelf());
        } else unhandled(msg);
	}

    private void onReceivePostTraining(Object msg) {
        if (msg instanceof WorkerActor.PostTrainingDone) {
            innerState.workerCount++;
            log("worker counter: " + innerState.workerCount);
            if (innerState.workerCount == conf.groupSize()) {
                log("tell monitor that training done");
                this.monitor.tell(new TrainingDone(groupIndex), getSelf());
                setState(State.DONE);
            }
        }
    }

	/*
	 * State Transition
	 */
	private void setState(State newState) {
		stateTransition(innerState.currentState, newState);
	}
	
	private void stateTransition(State old, State next) {
        if (old == State.CREATED && next == State.REGISTERED) {
            innerState.workerCount = 0;
            innerState.dataBuses = new ActorRef[conf.groupSize()];
        }
        else if (old == State.REGISTERED && next == State.TRAINING) {
        }
        else if (old == State.TRAINING && next == State.TRAINING) {
        }
        log("State Transition from " + old.toString() + " to " + next.toString());
		innerState.currentState = next;
	}

    private void trainWithNextSample() {
        if (setupInput()) {
            ++innerState.currentTrainingProgress;
            if (innerState.currentTrainingProgress - innerState.lastReportProgress >= conf.progressStepSize()) {
                monitor.tell(new ProgressReport(innerState.currentTrainingProgress - innerState.lastReportProgress,
                        innerState.currentTrainingProgress, groupIndex), getSelf());
                innerState.lastReportProgress = innerState.currentTrainingProgress;
            }
            broadcast(new SampleStart());
        }
        else {
            stopTraining();
        }
    }

    private boolean setupInput() {

        while(innerState.currentWorkerAsInput < conf.groupSize()) {
            if (broadcastAndWait(new InputSetup(innerState.currentWorkerAsInput, conf.miniBatchSize()))) {
                return true;
            }
            innerState.currentWorkerAsInput++;
        }

        return false;
    }

    private void stopTraining() {
        setState(State.POST_TRAINING);
        innerState.workerCount = 0;
        broadcast(new PostTraining());
    }

    public boolean broadcastAndWait(Object msg) {

        LinkedList<Future<Object>> responseFutures = new LinkedList<Future<Object>>();
        for (int i = 0; i < conf.groupSize(); i++) {
            responseFutures.add(Patterns.ask(workers[i], msg, Constants.DATA_FUTURE_TIMEOUT));
        }
        Future<Iterable<Object>> responsesFuture = sequence(responseFutures, getContext().dispatcher());

        try {
            log("waiting broadcast result");
            Iterable<Object> responses = Await.result(responsesFuture, Constants.DATA_FUTURE_TIMEOUT_DURATION);
            int successNumber = 0;
            for (Object response : responses) {
                WorkerActor.RequestComplete res = (WorkerActor.RequestComplete) response;
                successNumber += res.success? 1 : 0;
                log("broadcast response from worker " + res.workerIndex + ", result=" + res.success);
            }

            return successNumber == conf.groupSize();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("no result.");
            //return null; // TODO Return null?
        }
    }

    private void broadcast(Object msg) {
        log("broadcast: " + msg);
        for (ActorRef worker : workers) {
            log("to: " + worker);
            worker.tell(msg, getSelf());
        }
    }

    private void log(String msg) {
        Logger.InfoLog(msg, Logger.Role.WORKER_LEAD, this.groupIndex);
    }
}
