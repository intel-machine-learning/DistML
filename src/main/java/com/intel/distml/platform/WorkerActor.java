package com.intel.distml.platform;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

import akka.actor.*;
import akka.io.Tcp;
import akka.io.TcpMessage;
import akka.japi.Creator;

import com.intel.distml.api.*;
import com.intel.distml.api.databus.DataBus;
import com.intel.distml.platform.TrainingConf;
import com.intel.distml.api.DMatrix;
import com.intel.distml.util.KeyCollection;
import com.intel.distml.util.Logger;
import com.intel.distml.util.Matrix;
import scala.concurrent.duration.Duration;

/**
 * Created by yunlong on 12/13/14.
 */
public class WorkerActor<T> extends UntypedActor {
	/*
	 * Messages
	 */
    public static class RegisterRequest extends DistMLMessage {
        private static final long serialVersionUID = 1L;

        final public int globalWorkerIndex;
        public RegisterRequest(int globalWorkerIndex) {
            this.globalWorkerIndex = globalWorkerIndex;
        }
    }

    public static class CheckProgress extends DistMLMessage {
        private static final long serialVersionUID = 1L;

        public CheckProgress(){
        }
    }

    public static class PostTrainingDone extends DistMLMessage {
        private static final long serialVersionUID = 1L;

        public PostTrainingDone(){
        }
    }

    public static class ProgressReport extends DistMLMessage {
        private static final long serialVersionUID = 1L;

        final public int trained;
        final public int totalTrained;
        final public int workerIndex;
        public ProgressReport(int trained, int totalTrained, int workerIndex) {
            this.trained = trained;
            this.totalTrained = totalTrained;
            this.workerIndex = workerIndex;
        }
    }

    Iterator<T> samples;
    private int sampleWorkerIndex;

    ActorSelection monitor;
    //ActorRef[] parameterServers;
	Model model;

    TrainingConf conf;

    DataBusImpl dataBus;
    int workerIndex;

	/*
	 * Dynamic State
	 */
	static enum State {
		CREATED,
		TRAINING,
		DONE
		// New states later
	}

	// Worker Lead State
	class InnerStateData {
		State currentState = State.CREATED;
        ActorRef leadRequestSender;

        int currentTrainingProgress = 0;
        int lastReportProgress = 0;
        int dataBusInitCounter = 0;
	}
	InnerStateData innerState = new InnerStateData();

    DataBus dataBusProxy = new DataBus() {
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

    };

    Cancellable progressReminder;
    Thread trainingThread = new Thread() {

        @Override
        public void run() {
            while(samples.hasNext()) {
                //System.out.println("compute");
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

            model.postTraining(workerIndex, dataBus);
            log("training thread stopped.");
        }
    };

    Thread postTrainingThread = new Thread() {

        @Override
        public void run() {
            model.postTraining(workerIndex, dataBusProxy);
            getSelf().tell(new PostTrainingDone(), getSelf());
        }
    };

	public WorkerActor(String monitorPath, Model model, int workerIndex
                       , Iterator<T> samples, TrainingConf conf) {
        this.monitor = getContext().actorSelection(monitorPath);
        this.model = model;
        this.workerIndex = workerIndex;
        this.samples = samples;
        this.conf = conf;

        this.monitor.tell(new RegisterRequest(this.workerIndex), getSelf());
        log("Worker register to monitor: " + monitorPath);

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
        log("onReceive: " + msg + ", " + innerState.currentState + ", " + conf.progressStepSize());

        if (innerState.currentState == State.CREATED) {
            if (msg instanceof MonitorActor.RegisterResponse) {
                MonitorActor.RegisterResponse res = (MonitorActor.RegisterResponse) msg;

                final ActorRef tcpManager = Tcp.get(getContext().system()).manager();

                ActorRef[] clients = new ActorRef[conf.psCount()];
                for (int i = 0; i < conf.psCount(); i++) {
                    clients[i] = getContext().actorOf(DataRelay.props(getSelf()));
                    tcpManager.tell(TcpMessage.connect(res.psAddrs[i]), clients[i]);
                }
                dataBus = new DataBusImpl(clients, model, getContext());
                innerState.dataBusInitCounter = 0;

            } else if (msg instanceof Tcp.Connected) {
                innerState.dataBusInitCounter++;
                if (innerState.dataBusInitCounter == conf.psCount()) {
                    initModel();

                    model.preTraining(workerIndex, dataBus);

                    setState(State.TRAINING);

                    progressReminder = getContext().system().scheduler().schedule(Duration.Zero(), Duration.create(5, TimeUnit.SECONDS), getSelf(), new CheckProgress(), getContext().dispatcher(), getSelf());
                    trainingThread.start();
                }
            }
        } else if (innerState.currentState == State.TRAINING) {
            if (msg instanceof MonitorActor.VariableChange) {
                MonitorActor.VariableChange vc = (MonitorActor.VariableChange)msg;
                model.variableChanged(vc.name, vc.value);
                return;
            }
            else if (msg instanceof CheckProgress) {
                if (innerState.currentTrainingProgress - innerState.lastReportProgress >= conf.progressStepSize()) {
                    log("progress: " + innerState.currentTrainingProgress + ", " + conf.progressStepSize());
                    monitor.tell(new ProgressReport(innerState.currentTrainingProgress - innerState.lastReportProgress,
                            innerState.currentTrainingProgress, workerIndex), getSelf());
                    innerState.lastReportProgress += conf.progressStepSize();
                }

                if (!trainingThread.isAlive()) {
                    //log("Tell worker lead training is done");
                    //monitor.tell(new TrainingDone(workerIndex), getSelf());

                    progressReminder.cancel();
                    getContext().stop(getSelf());
                }

                return;
            }
        }
        else unhandled(msg);
	}

    @Override
    public void postStop() {
        log("Worker stopped");
        getContext().system().shutdown();
    }

    private void initModel() {
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
    }
    /*
     * State Transition
     */
    private void setState(State newState) {
        innerState.currentState = newState;
        if (newState == State.TRAINING) {
            innerState.currentTrainingProgress = 0;
            innerState.lastReportProgress = 0;
        }
    }

    private void log(String msg) {
        Logger.InfoLog(msg, Logger.Role.WORKER, this.workerIndex);
    }
}

