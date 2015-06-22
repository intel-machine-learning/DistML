package com.intel.distml.model.demo_echo;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.io.Tcp;
import com.typesafe.config.ConfigFactory;

/**
 * Created by lq on 6/22/15.
 */
public class Demo {
    public void run() {
        ActorSystem mySystem = ActorSystem.create("mySystem");
        ActorRef tcpManager = Tcp.get(mySystem).getManager();


        Props serverProps = Props.create(Echo_server.class);
        ActorRef server = mySystem.actorOf(serverProps, "server");


        Props receiverProps = Props.create(Echo_client.class, tcpManager);
        ActorRef receiver = mySystem.actorOf(receiverProps, "receiver");
        receiver.tell(1234, server);//ActorRef.noSender());
        mySystem.awaitTermination();
    }
}
