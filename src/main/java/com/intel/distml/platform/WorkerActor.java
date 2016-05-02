package com.intel.distml.platform;

import akka.actor.*;
import akka.japi.Creator;
import com.intel.distml.api.Session;
import com.intel.distml.api.Model;
import com.intel.distml.util.Logger;

import java.io.Serializable;

/**
 * Created by yunlong on 12/13/14.
 */
public class WorkerActor extends UntypedActor {

    public static final int CMD_DISCONNECT      = 1;
    public static final int CMD_STOP            = 2;
    public static final int CMD_PS_TERMINATED   = 3;
    public static final int CMD_PS_AVAILABLE    = 4;

    public static class Progress implements Serializable {
        private static final long serialVersionUID = 1L;

        final public int sampleCount;
        public Progress(int sampleCount) {
            this.sampleCount = sampleCount;
        }
    }

    public static class Command implements Serializable {
        private static final long serialVersionUID = 1L;

        final public int cmd;
        public Command(int cmd) {
            this.cmd = cmd;
        }
    }

    public static class PsTerminated extends Command {
        private static final long serialVersionUID = 1L;

        final public int index;
        public PsTerminated(int index) {
            super(CMD_PS_TERMINATED);
            this.index = index;
        }
    }

    public static class PsAvailable extends Command {
        private static final long serialVersionUID = 1L;

        final public int index;
        final public String addr;
        public PsAvailable(int index, String addr) {
            super(CMD_PS_AVAILABLE);
            this.index = index;
            this.addr = addr;
        }
    }

    public static class RegisterRequest implements Serializable {
        private static final long serialVersionUID = 1L;

        final public int workerIndex;
        public RegisterRequest(int workerIndex) {
            this.workerIndex = workerIndex;
        }
    }

    public static class AppRequest implements Serializable {
        private static final long serialVersionUID = 1L;

        public boolean done;
        public AppRequest() {
            done = false;
        }

        public String toString() {
            return "DriverRequest";
        }
    }

    public static class IterationDone extends AppRequest {
        private static final long serialVersionUID = 1L;

        int iteration;
        double cost;
        public IterationDone(int iteration, double cost) {
            this.iteration = iteration;
            this.cost = cost;
        }

        public String toString() {
            return "IterationDone";
        }
    }

    private ActorSelection monitor;
    private Model model;
    private int psCount;
    private Session de;

    private String[] psAddrs;
    private int workerIndex;

    private AppRequest pendingRequest;

	public WorkerActor(final Session de, Model model, String monitorPath, int workerIndex) {
        this.monitor = getContext().actorSelection(monitorPath);
        this.workerIndex = workerIndex;
        this.model = model;
        this.de = de;

        this.monitor.tell(new RegisterRequest(this.workerIndex), getSelf());
        log("Worker " + workerIndex + " register to monitor: " + monitorPath);
	}

	public static <ST> Props props(final Session de,final Model model, final String monitorPath, final int index) {
		return Props.create(new Creator<WorkerActor>() {
			private static final long serialVersionUID = 1L;
			public WorkerActor create() throws Exception {
				return new WorkerActor(de, model, monitorPath, index);
			}
		});
	}

	@Override
	public void onReceive(Object msg) {
        log("onReceive: " + msg);

        if (msg instanceof MonitorActor.RegisterResponse) {
            MonitorActor.RegisterResponse res = (MonitorActor.RegisterResponse) msg;
            this.psAddrs = res.psAddrs;
            psCount = psAddrs.length;

            de.dataBus = new WorkerAgent(model, psAddrs);
        }
        else if (msg instanceof Progress) {
            this.monitor.tell(msg, getSelf());
        }
        else if (msg instanceof IterationDone) {
            IterationDone req = (IterationDone) msg;
            pendingRequest = (AppRequest)msg;
            monitor.tell(new MonitorActor.SSP_IterationDone(workerIndex, req.iteration, req.cost), getSelf());
        }
        else if (msg instanceof MonitorActor.SSP_IterationNext) {
            assert(pendingRequest != null);
            pendingRequest.done = true;
        }
        else if (msg instanceof PsAvailable) {
            PsAvailable ps = (PsAvailable) msg;
            de.dataBus.psAvailable(ps.index, ps.addr);
        }
        else unhandled(msg);
	}

    private void log(String msg) {
        Logger.info(msg, "Worker-" + workerIndex);
    }
}

