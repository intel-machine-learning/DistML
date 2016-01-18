package com.intel.distml.platform;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.LinkedList;

import akka.actor.*;
import akka.japi.Creator;

import akka.pattern.Patterns;
import com.intel.distml.api.Model;
import com.intel.distml.util.*;
import scala.concurrent.Await;
import scala.concurrent.Future;

import static akka.dispatch.Futures.sequence;

public class MonitorActor extends UntypedActor {

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

    private int psCount;

    private ActorRef[] parameterServers;
    private String[] psAddrs;
    private ActorRef[] serverDataBuses;

    private LinkedList<ActorRef> workers;

    private Model model;
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

    public MonitorActor(Model model) {
        this.psCount = model.psCount;
        this.parameterServers = new ActorRef[psCount];
        this.psAddrs = new String[psCount];
        this.workers = new LinkedList<ActorRef>();

        this.model = model;

        setState(State.CREATED);
        innerState.psCounter = 0;

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
        log("onReceive: " + msg + ", " + innerState.currentState + ", "
                + innerState.psCounter + ", "
                + getSender() );
        if (innerState.currentState == State.CREATED) {
            if (msg instanceof ParameterServerActor.RegisterRequest) {
                ParameterServerActor.RegisterRequest req = (ParameterServerActor.RegisterRequest)msg;
                log("Parameter server registered: " + getSender());

                parameterServers[req.parameterServerIndex] = getSender();
                psAddrs[req.parameterServerIndex] = req.addr;
                ++innerState.psCounter;

                log("counter: " + innerState.psCounter + ", " + psCount);
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
                    model.psReady = true;
                    //workerStarter.tell(new ParameterServersReady(parameterServers), getSelf());
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
            } else if (msg instanceof TrainingDone) {
                setState(State.DONE);
                stopAll();
            }

            else unhandled(msg);
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
            Logger.error("Timeout when stopping actors. ", "Monitor");
        }
    }

    private void log(String msg) {
        Logger.info(msg, "Monitor");
    }
}