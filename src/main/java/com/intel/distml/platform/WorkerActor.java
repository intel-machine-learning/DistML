package com.intel.distml.platform;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

import akka.actor.*;
import akka.io.Tcp;
import akka.io.TcpMessage;
import akka.japi.Creator;

import com.intel.distml.api.*;
import com.intel.distml.api.databus.DataBus;
import com.intel.distml.api.DMatrix;
import com.intel.distml.model.word2vec.Word2VecModel;
import com.intel.distml.util.DList;
import com.intel.distml.util.KeyCollection;
import com.intel.distml.util.Logger;
import com.intel.distml.util.Matrix;
import scala.concurrent.duration.Duration;

import javax.management.RuntimeErrorException;

/**
 * Created by yunlong on 12/13/14.
 */
public class WorkerActor<T> extends UntypedActor {

    private static final int CMD_DISCONNECT = 1;
    private static final int CMD_STOP       = 2;

    static class Command extends DistMLMessage {
        private static final long serialVersionUID = 1L;

        final public int cmd;
        public Command(int cmd) {
            this.cmd = cmd;
        }
    }

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
    DList result;
	Model model;

    TrainingContext context;

    InetSocketAddress[] psAddrs;   // parameter servers
    DataBusImpl dataBus;
    ActorRef[] connections;
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
                LinkedList<Object> sampleList = new LinkedList<Object>();
                for (int i = 0; i < context.miniBatchSize ; i++) {
                    if (!samples.hasNext()) break;
                    Object s = samples.next();
                    //System.out.println("add sample: [" + s + "]");
                    sampleList.add(s);
                }
                innerState.currentTrainingProgress += sampleList.size();

                log("batch training: " + sampleList.size());
                if (model.dataSetImmutable) {
                    model.compute(model.transformSamples(sampleList), workerIndex, dataBusProxy, context.currentIter);
                }
                else {
                    model.compute(model.transformSamples(sampleList), workerIndex, dataBusProxy, context.currentIter, result);
                }
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

	public WorkerActor(Model model, String monitorPath, int workerIndex
                       , Iterator<T> samples, TrainingContext context, DList result) {
        this.monitor = getContext().actorSelection(monitorPath);
        this.workerIndex = workerIndex;
        this.samples = samples;
        this.context = context;
        this.result = result;
        this.model = model;

        this.monitor.tell(new RegisterRequest(this.workerIndex), getSelf());
        log("Worker register to monitor: " + monitorPath);

        setState(State.CREATED);
	}

	public static <ST> Props props(final Model model, final String monitorPath,
        final int index, final Iterator<ST> samples, final TrainingContext conf, final DList result) {
		return Props.create(new Creator<WorkerActor>() {
			private static final long serialVersionUID = 1L;
			public WorkerActor create() throws Exception {
				return new WorkerActor(model, monitorPath, index, samples, conf, result);
			}
		});
	}

	@Override
	public void onReceive(Object msg) {
        log("onReceive: " + msg + ", " + innerState.currentState + ", " + context.progressStepSize);

        if (innerState.currentState == State.CREATED) {
            if (msg instanceof MonitorActor.RegisterResponse) {
                MonitorActor.RegisterResponse res = (MonitorActor.RegisterResponse) msg;
                this.psAddrs = res.psAddrs;

                final ActorRef tcpManager = Tcp.get(getContext().system()).manager();
                connections = new ActorRef[context.psCount];
                for (int i = 0; i < context.psCount; i++) {
                    connections[i] = getContext().actorOf(DataRelay.props(getSelf()));
                    tcpManager.tell(TcpMessage.connect(psAddrs[i]), connections[i]);
                }
                dataBus = new DataBusImpl(connections, model, getContext());

                innerState.dataBusInitCounter = 0;

            } else if (msg instanceof Tcp.Connected) {
                innerState.dataBusInitCounter++;
                if (innerState.dataBusInitCounter == context.psCount) {
                    initModel();

                    model.preTraining(workerIndex, dataBus);

                    setState(State.TRAINING);

                    progressReminder = getContext().system().scheduler().schedule(Duration.Zero(), Duration.create(10, TimeUnit.MILLISECONDS), getSelf(), new CheckProgress(), getContext().dispatcher(), getSelf());
                    log("thread state: " + trainingThread.getState());
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
                if (innerState.currentTrainingProgress - innerState.lastReportProgress >= context.progressStepSize) {
                    log("progress: " + innerState.currentTrainingProgress + ", " + context.progressStepSize);
                    monitor.tell(new ProgressReport(innerState.currentTrainingProgress - innerState.lastReportProgress,
                            innerState.currentTrainingProgress, workerIndex), getSelf());
                    //innerState.lastReportProgress += context.progressStepSize;
                    innerState.lastReportProgress = innerState.currentTrainingProgress;
                }

                if (trainingThread.getState() == Thread.State.TERMINATED) {
                    if (innerState.currentTrainingProgress > innerState.lastReportProgress) {
                        log("progress: " + innerState.currentTrainingProgress + ", " + context.progressStepSize);
                        monitor.tell(new ProgressReport(innerState.currentTrainingProgress - innerState.lastReportProgress,
                                innerState.currentTrainingProgress, workerIndex), getSelf());
                        innerState.lastReportProgress = innerState.currentTrainingProgress;
                    }

                    //log("Tell worker lead training is done");
                    progressReminder.cancel();
                    //monitor.tell(new MonitorActor.WorkerIterationDone(workerIndex, context.currentIter), getSelf());
                    innerState.currentState = State.DONE;


                    getContext().system().scheduler().scheduleOnce(Duration.create(100, TimeUnit.MILLISECONDS), getSelf(), new Command(CMD_DISCONNECT), getContext().dispatcher(), getSelf());
                }

                return;
            }
/*
            else if (msg instanceof MonitorActor.Stop) {
                log("stop: " + ((MonitorActor.Stop)msg).time);
                if (trainingThread.isAlive()) {
                    throw new RuntimeException("Incorrect stop command received");
                }
                getContext().stop(getSelf());
            }
*/
        } else if (innerState.currentState == State.DONE) {
            Command cmd = (Command) msg;
            if (cmd.cmd == CMD_DISCONNECT) {
                for (ActorRef c : connections) {
                    c.tell(new DataRelay.CloseAtOnce(), getSelf());
                }
                getContext().system().scheduler().scheduleOnce(Duration.create(100, TimeUnit.MILLISECONDS), getSelf(), new Command(CMD_STOP), getContext().dispatcher(), getSelf());
            }
            else if (cmd.cmd == CMD_STOP) {
                getContext().stop(getSelf());
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

