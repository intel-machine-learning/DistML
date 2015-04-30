package com.intel.distml.platform.worker;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

import akka.actor.*;
import akka.japi.Creator;

import akka.pattern.Patterns;
import com.intel.distml.api.*;
import com.intel.distml.api.databus.DataBus;
import com.intel.distml.platform.TrainingConf;
import com.intel.distml.platform.monitor.MonitorActor;
import com.intel.distml.api.DMatrix;
import com.intel.distml.util.Constants;
import com.intel.distml.util.KeyCollection;
import com.intel.distml.util.Logger;
import com.intel.distml.transport.DataBusImpl;
import com.intel.distml.util.Matrix;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

/**
 * Created by yunlong on 12/13/14.
 */
public class WorkerActor<T> extends UntypedActor {
	/*
	 * Messages
	 */
    public static class RegisterRequest implements Serializable {
        private static final long serialVersionUID = 1L;

        final public int globalWorkerIndex;
        public RegisterRequest(int globalWorkerIndex) {
            this.globalWorkerIndex = globalWorkerIndex;
        }
    }

    public static class CheckProgress implements Serializable {
        private static final long serialVersionUID = 1L;

        public CheckProgress(){
        }
    }

    public static class PostTrainingDone implements Serializable {
        private static final long serialVersionUID = 1L;

        public PostTrainingDone(){
        }
    }

    public static class RequestComplete implements Serializable {
        private static final long serialVersionUID = 1L;

        final public int workerIndex;
        final public boolean success;
        public RequestComplete(int workerIndex, boolean success) {
            this.workerIndex = workerIndex;
            this.success = success;
        }
    }

    Iterator<T> samples;
    private int sampleWorkerIndex;

    ActorSelection monitor;
	ActorRef workerLead;
	ActorRef[] workers;
    ActorRef[] parameterServers;
	Model model;

    TrainingConf conf;


    ActorRef dataBusActor;
    DataBusImpl dataBus;
    int globalWorkerIndex;
	int workerIndex;

	/*
	 * Dynamic State
	 */
	static enum State {
		CREATED,        // just created, next step is to register to monitor
        REGISTERED,     // registered, but parameters not fetched from server
		TRAINING,       //
		DONE
		// New states later
	}

	// Worker Lead State
	class InnerStateData {
		State currentState = State.CREATED;
        ActorRef leadRequestSender;

        int currentTrainingProgress = 0;
        int lastReportProgress = 0;
	}
	InnerStateData innerState = new InnerStateData();

    DataBus dataBusProxy = new DataBus() {
        public Matrix fetchFromWorker(String matrixName) {
            return dataBus.fetchFromWorker(matrixName, KeyCollection.ALL, KeyCollection.ALL);
        }

        public Matrix fetchFromWorker(String matrixName, KeyCollection rowKeys) {
            return dataBus.fetchFromWorker(matrixName, rowKeys, KeyCollection.ALL);
        }

        public Matrix fetchFromWorker(String matrixName, KeyCollection rowKeys, KeyCollection colsKeys) {
            return dataBus.fetchFromWorker(matrixName, rowKeys, colsKeys);
        }

        public Matrix fetchFromServer(String matrixName) {
            return dataBus.fetchFromServer(matrixName, KeyCollection.ALL, KeyCollection.ALL);
        }

        public Matrix fetchFromServer(String matrixName, KeyCollection rowKeys) {
            return dataBus.fetchFromServer(matrixName, rowKeys, KeyCollection.ALL);
        }

        public Matrix fetchFromServer(String matrixName, KeyCollection rowKeys, KeyCollection colsKeys) {
            return dataBus.fetchFromServer(matrixName, rowKeys, colsKeys);
        }

        public void pushUpdate(String matrixName, Matrix update) {
            dataBus.pushUpdate(matrixName, update);
        }

        public void barrier() {
            if (conf.groupSize() == 1) {
                return;
            }
            waitBarrier();
        }
//
//    public void iterationDone();
    };

    Cancellable progressReminder;
    Thread trainingThread = new Thread() {

        @Override
        public void run() {
            while(samples.hasNext()) {
                LinkedList<Object> sampleList = new LinkedList<Object>();
                for (int i = 0; i < conf.miniBatchSize() ; i++) {
                    if (!samples.hasNext()) break;
                    Object s = samples.next();
                    //System.out.println("add sample: [" + s + "]");
                    sampleList.add(s);
                }
                innerState.currentTrainingProgress += sampleList.size();

                model.compute(model.transformSamples(sampleList), workerIndex, dataBusProxy);
            }
        }
    };

    Thread postTrainingThread = new Thread() {

        @Override
        public void run() {
            model.postTraining(workerIndex, dataBusProxy);
            getSelf().tell(new PostTrainingDone(), getSelf());
        }
    };

	public WorkerActor(String monitorPath, Model model, int globalWorkerIndex
                       , Iterator<T> samples, TrainingConf conf) {
        this.monitor = getContext().actorSelection(monitorPath);
        this.model = model;
        this.globalWorkerIndex = globalWorkerIndex;
        this.samples = samples;
        this.conf = conf;

        this.monitor.tell(new RegisterRequest(this.globalWorkerIndex), getSelf());
        log("Worker register to monitor");

        setState(State.CREATED);
	}

	public static <ST> Props props(final String monitorPath, final Model model,
        final int index, final Iterator<ST> samples, final TrainingConf conf) {
		return Props.create(new Creator<WorkerActor>() {
			private static final long serialVersionUID = 1L;
			public WorkerActor create() throws Exception {
				return new WorkerActor(monitorPath, model, index, samples, conf);
			}
		});
	}

	@Override
	public void onReceive(Object msg) {
        log("onReceive: " + msg);
        if (msg instanceof MonitorActor.VariableChange) {
            MonitorActor.VariableChange vc = (MonitorActor.VariableChange)msg;
            model.variableChanged(vc.name, vc.value);
            return;
        }
        else if (msg instanceof CheckProgress) {
            if (innerState.currentTrainingProgress - innerState.lastReportProgress >= conf.progressStepSize()) {
                log("progress: " + innerState.currentTrainingProgress + ", " + conf.progressStepSize());
                monitor.tell(new WorkerLeadActor.ProgressReport(innerState.currentTrainingProgress - innerState.lastReportProgress,
                        innerState.currentTrainingProgress, workerIndex), getSelf());
                innerState.lastReportProgress += conf.progressStepSize();
            }

            if (!trainingThread.isAlive()) {
                log("Tell worker lead training is done");
                workerLead.tell(new WorkerLeadActor.WorkerTrainingDone(workerIndex), getSelf());
                progressReminder.cancel();
            }

            return;
        }
        else if (msg instanceof PostTrainingDone) {
            log("Tell worker lead post training is done");
            workerLead.tell(msg, getSelf());
        }

        if (innerState.currentState == State.CREATED)
            onReceiveAfterCreated(msg);
        else if (innerState.currentState == State.REGISTERED)
            onReceiveAfterRegistered(msg);
        else if (innerState.currentState == State.TRAINING)
            onReceiveTraining(msg);
        else unhandled(msg);
	}

    @Override
    public void postStop() {
        log("Worker stopped");
        getContext().system().shutdown();
    }

    /*
     * Message Handler
     */
    private void onReceiveAfterCreated(Object msg) {
        if (msg instanceof MonitorActor.RegisterResponse) {
            MonitorActor.RegisterResponse workerInfo = (MonitorActor.RegisterResponse) msg;
            log("Received Group Info");

            this.workerIndex = workerInfo.workerIndex;
            this.workerLead = workerInfo.workerLead;
            this.workers = workerInfo.workers;
            this.parameterServers = workerInfo.parameterServers;
            ActorRef removeMe = parameterServers[0];

            // Initialize worker here
            for (DMatrix m : model.dataMap.values()) {
                if (!m.hasFlag(DMatrix.FLAG_ON_WORKER)) {
                    continue;
                }

                PartitionInfo info = m.workerPartitions();
                KeyCollection keys = m.getRowKeys();  // if not partitioned or copied
                if (info != null) {
                    if (info.type == PartitionInfo.Type.PARTITIONED) {
                        keys = info.getPartition(workerIndex).keys;
                    }
                    else if ((info.type == PartitionInfo.Type.EXCLUSIVE) && (info.exclusiveIndex != workerIndex)) {
                        return;
                    }
                }

                m.initOnWorker(this.workerIndex, keys);
            }

            setState(State.REGISTERED);

            // Initialize WorkerDataBusActor
            this.dataBusActor = getContext().actorOf(WorkerDataBusActor.props(workerIndex, model, samples));

            log("Tell worker lead information of DataBus");
            workerLead.tell(new WorkerLeadActor.CheckInRequest(workerIndex, getSelf(), dataBusActor), getSelf());

        } else {
            Logger.ErrorLog("unexpected message: " + msg + ", " + innerState, Logger.Role.WORKER, workerIndex);
            unhandled(msg);
        }
    }

    /*
     * Message Handler
     */
    private void onReceiveAfterRegistered(Object msg) {
        if (msg instanceof WorkerLeadActor.CheckInResponse) {
            WorkerLeadActor.CheckInResponse res = (WorkerLeadActor.CheckInResponse) msg;
            log("Received Group Info");

            // TODO Use actor dispatcher current now, we can set a specific dispatcher for blocking future later!
            this.dataBus = new DataBusImpl(workerIndex, res.dataBuses, parameterServers, model, getContext());

            log("Tell worker lead worker is ready");
            getSender().tell(new RequestComplete(workerIndex, true), getSelf());

        } else if (msg instanceof WorkerLeadActor.PreTraining) {

            model.preTraining(workerIndex, dataBusProxy);
            getSender().tell(new RequestComplete(workerIndex, true), getSelf());


        } else if (msg instanceof WorkerLeadActor.TrainingStart) {
            setState(State.TRAINING);
            getSender().tell(new RequestComplete(workerIndex, true), getSelf());

            // todo async run, or use thread to run training to avoid blocking.
            if (((WorkerLeadActor.TrainingStart)msg).runToComplete) {
                log("start training and run to complete");
                innerState.currentTrainingProgress = 0;
                innerState.lastReportProgress = 0;

                progressReminder = getContext().system().scheduler().schedule(Duration.Zero(), Duration.create(5, TimeUnit.SECONDS), getSelf(), new CheckProgress(),getContext().dispatcher(), getSelf());
                trainingThread.start();
            }

        } else {
            Logger.ErrorLog("unexpected message: " + msg + ", " + innerState, Logger.Role.WORKER, workerIndex);
            unhandled(msg);
        }
    }

    private boolean waitBarrier() {

        WorkerLeadActor.Barrier req = new WorkerLeadActor.Barrier(workerIndex);
        Future<Object> responseFuture = Patterns.ask(workerLead, req, Constants.DATA_FUTURE_TIMEOUT);  //todo use async way to allow long time wait

        try {
            log("waiting barrier over");
            WorkerLeadActor.BarrierOver responses = (WorkerLeadActor.BarrierOver) Await.result(responseFuture, Constants.DATA_FUTURE_TIMEOUT_DURATION);
            log("barrier over received");
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("time out in barrier.");
        }
    }

    private void onReceiveTraining(Object msg) {

//        if (msg instanceof WorkerLeadActor.FetchParameter) { // Fetch parameters before each iteration
////
////            for (int layerIndex = 0; layerIndex < model.layers.size(); ++layerIndex) {
//////                Matrix matrix = dataBus.fetchFromServer(layerIndex, Layer.MATRIX_PARAM,
//////                        KeyCollection.ALL_KEYS, KeyCollection.ALL_KEYS);
//////
//////                model.getLayer(layerIndex).getMatrix(Layer.MATRIX_PARAM).setLocalCache(matrix);
//////
////                dataBus.initParamsFromServer();
////            }
//
//            getSender().tell(new RequestComplete(workerIndex, true), getSelf());
//
//        } else
        if (msg instanceof WorkerLeadActor.InputSetup) {
            WorkerLeadActor.InputSetup cmd = (WorkerLeadActor.InputSetup) msg;

            if (cmd.workerIndex == workerIndex) {
                if (!samples.hasNext()) {
                    log("no sample left in this worker.");
                    getSender().tell(new RequestComplete(workerIndex, false), getSelf());
                    return;
                }

                LinkedList<Object> sampleList = new LinkedList<Object>();
                for (int i = 0; i < cmd.sampleNumber; i++) {
                    if (!samples.hasNext()) break;
                    Object s = samples.next();
                    System.out.println("add sample: [" + s + "]");
                    sampleList.add(s);
                }

                model.setCache(Model.MATRIX_SAMPLE, model.transformSamples(sampleList));
            }

            this.sampleWorkerIndex = cmd.workerIndex;
            getSender().tell(new RequestComplete(workerIndex, true), getSelf());

        } else if (msg instanceof WorkerLeadActor.PostTraining) {
            postTrainingThread.start();
        } else if (msg instanceof WorkerLeadActor.SampleStart) {

            if (sampleWorkerIndex == workerIndex) {
                Matrix samplesToTrain = model.getCache(Model.MATRIX_SAMPLE);
                model.compute(samplesToTrain, workerIndex, dataBusProxy);
            }
            else {
                Matrix samplesToTrain = dataBus.fetchSamples(sampleWorkerIndex);
                model.compute(samplesToTrain, workerIndex, dataBusProxy);
            }

			getSender().tell(new WorkerLeadActor.SampleDone(workerIndex, true), getSelf());

//        } else if (msg instanceof WorkerLeadActor.PushParameter) {
//
//            dataBus.pushUpdatesAndWait();
//            getSender().tell(new RequestComplete(workerIndex, true), getSelf());

        } else {
            Logger.ErrorLog("unexpected message: " + msg + ", " + innerState, Logger.Role.WORKER, workerIndex);
            unhandled(msg);
        }

    }

    /*
     * State Transition
     */
    private void setState(State newState) {
        stateTransition(innerState.currentState, newState);
    }

    private void stateTransition(State old, State next) {
        // Do state transition here
        log("State Transition from " + old.toString() + " to " + next.toString());
        innerState.currentState = next;
    }

    private void log(String msg) {
        Logger.InfoLog(msg, Logger.Role.WORKER, this.globalWorkerIndex);
    }
}

