package com.intel.distml.api;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.intel.distml.platform.MonitorActor;
import com.intel.distml.platform.WorkerActor;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Created by yunlong on 12/9/15.
 */
public class Session {

    static final String ACTOR_SYSTEM_CONFIG =
            "akka.actor.provider=\"akka.remote.RemoteActorRefProvider\"\n" +
            "akka.remote.netty.tcp.port=0\n" +
            "akka.remote.log-remote-lifecycle-events=off\n" +
            "akka.log-dead-letters=off\n" +
            "akka.io.tcp.direct-buffer-size = 2 MB\n" +
            "akka.io.tcp.trace-logging=off\n" +
            "akka.remote.netty.tcp.maximum-frame-size=4126935";


    public ActorSystem workerActorSystem;
    public DataBus dataBus;
    public ActorRef monitor;
    public ActorRef worker;
    public Model model;

    public Session(Model model, String monitorPath, int workerIndex) {
        this.model = model;
        connect(monitorPath, workerIndex);
    }

    public void connect(String monitorPath, int workerIndex) {
        dataBus = null;

        String WORKER_ACTOR_SYSTEM_NAME = "worker";
        String WORKER_ACTOR_NAME = "worker";

        Config cfg = ConfigFactory.parseString(ACTOR_SYSTEM_CONFIG);
        workerActorSystem = ActorSystem.create(WORKER_ACTOR_SYSTEM_NAME, ConfigFactory.load(cfg));
        worker = workerActorSystem.actorOf(WorkerActor.props(this, model, monitorPath, workerIndex), WORKER_ACTOR_NAME);

        while(dataBus == null) {
            try {Thread.sleep(10); } catch (InterruptedException e) {}
        }
    }

    public void progress(int sampleCount) {
        worker.tell(new WorkerActor.Progress(sampleCount), null);
    }

    public void iterationDone(int iteration) {
        iterationDone(iteration, 0.0);
    }

    public void iterationDone(int iteration, double cost) {
        WorkerActor.IterationDone req = new WorkerActor.IterationDone(iteration, cost);
        worker.tell(req, null);
        while(!req.done) {
            try { Thread.sleep(100); } catch (Exception e) {}
        }
    }

    public void disconnect() {
        dataBus.disconnect();
        workerActorSystem.stop(worker);
        workerActorSystem.shutdown();
        dataBus = null;
    }

    public void discard() {
        worker.tell(new WorkerActor.Command(WorkerActor.CMD_DISCONNECT), null);
    }

    @Override
    public void finalize() {
        if (dataBus != null)
            disconnect();
    }

}
