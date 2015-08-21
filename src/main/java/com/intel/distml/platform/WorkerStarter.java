package com.intel.distml.platform;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.japi.Creator;
import com.intel.distml.api.Model;

import java.util.Iterator;

/**
 * Created by yunlong on 6/13/15.
 */
public class WorkerStarter extends UntypedActor {

    public static interface Callback {
        //void monitorReady();
        void parameterServersReady();
    }

    ActorSelection monitor;
    Callback callback;

    public WorkerStarter(String monitorPath, Callback callback) {
        monitor = getContext().actorSelection(monitorPath);
        this.callback = callback;

        this.monitor.tell(new MonitorActor.WorkerStarterRegister(), getSelf());
    }

    public static Props props(final String monitorPath, final Callback callback) {
        return Props.create(new Creator<WorkerStarter>() {
            public WorkerStarter create() throws Exception {
                return new WorkerStarter(monitorPath, callback);
            }
        });
    }

    @Override
    public void onReceive(Object msg) {
        if (msg instanceof MonitorActor.ParameterServersReady) {
            callback.parameterServersReady();
            System.out.println("telling monitor to stop training");
            monitor.tell(new MonitorActor.TrainingDone(), getSelf());

            getContext().stop(getSelf());
        }
    }
}
