package com.intel.distml.platform;

import akka.actor.*;
import akka.io.Tcp;
import akka.io.TcpMessage;
import akka.japi.Creator;
import com.intel.distml.api.Session;
import com.intel.distml.api.Model;
import com.intel.distml.api.DataBus;
import com.intel.distml.util.KeyCollection;
import com.intel.distml.util.Logger;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/**
 * Created by yunlong on 12/13/14.
 */
public class WorkerActor extends UntypedActor {

    public static final int CMD_DISCONNECT = 1;
    public static final int CMD_STOP       = 2;

    public static class Command implements Serializable {
        private static final long serialVersionUID = 1L;

        final public int cmd;
        public Command(int cmd) {
            this.cmd = cmd;
        }
    }

    public static class RegisterRequest implements Serializable {
        private static final long serialVersionUID = 1L;

        final public int globalWorkerIndex;
        public RegisterRequest(int globalWorkerIndex) {
            this.globalWorkerIndex = globalWorkerIndex;
        }
    }

    ActorSelection monitor;
	Model model;
    int psCount;
    Session de;

    String[] psAddrs;
    WorkerAgent agent;
//    DataBusImpl dataBus;
//    ActorRef[] connections;
    int workerIndex;

	/*
	 * Dynamic State
	 */
	static enum State {
		CREATED,
        READY
	}

    State currentState = State.CREATED;
    int dataBusInitCounter = 0;

	public WorkerActor(final Session de, Model model, String monitorPath, int workerIndex) {
        this.monitor = getContext().actorSelection(monitorPath);
        this.workerIndex = workerIndex;
        this.model = model;
        this.de = de;

        this.monitor.tell(new RegisterRequest(this.workerIndex), getSelf());
        log("Worker register to monitor: " + monitorPath);

        currentState = State.CREATED;
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
        log("onReceive: " + msg + ", " + currentState);

        if (currentState == State.CREATED) {
            if (msg instanceof MonitorActor.RegisterResponse) {
                MonitorActor.RegisterResponse res = (MonitorActor.RegisterResponse) msg;
                this.psAddrs = res.psAddrs;
                psCount = psAddrs.length;

                de.dataBus = new WorkerAgent(model, psAddrs);
            }
        }
        else unhandled(msg);
	}

    private void log(String msg) {
        Logger.info(msg, "Worker-" + workerIndex);
    }
}

