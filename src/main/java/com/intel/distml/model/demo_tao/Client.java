package com.intel.distml.model.demo_tao;

import akka.actor.*;
import akka.io.Tcp;
import akka.io.TcpMessage;
import akka.japi.Procedure;
import akka.util.ByteString;
import akka.util.ByteStringBuilder;
import com.typesafe.config.ConfigFactory;

import java.io.File;
import java.io.FileInputStream;
import java.net.InetSocketAddress;

/**
 * Created by taotaotheripper on 2015/5/8.
 */

public class Client extends UntypedActor implements Runnable{
    final InetSocketAddress remote;
    final ActorRef tcpManager = Tcp.get(getContext().system()).manager();

    Boolean useTCP = false;
    public Client() {
        this.remote = new InetSocketAddress("localhost", 1234);
        tcpManager.tell(TcpMessage.connect(remote), getSelf());
    }

    @Override
    public void onReceive(Object msg) throws Exception {
        if (msg instanceof Tcp.CommandFailed) {
            System.out.println("Connection failed.");
            getContext().stop(getSelf());
        } else if (msg instanceof Tcp.Connected) {
            System.out.println("Connected.");
            getSender().tell(TcpMessage.register(getSelf()), getSelf());
            send(getSender());
            getContext().become(connected(getSender()));
        } else {
            getSelf().tell(msg,getSender());
           // getSelf().tell(msg);
        }
    }

    int sentNumber = 0;
    long last = System.currentTimeMillis();
    ByteString content = null;
    private Procedure<Object> connected(final ActorRef connection) {
        return new Procedure<Object>() {
            @Override
            public void apply(Object msg) throws Exception {
                if (msg.equals(0)) {
                    System.out.println("Message Sent");
                } else if (msg instanceof Tcp.CommandFailed) {
                    System.out.println("Command Failed!");
                    // OS kernel socket buffer was full
                } else if (msg.equals("close")) {
                    connection.tell(TcpMessage.close(), getSelf());
                } else if (msg instanceof Tcp.ConnectionClosed) {
                    getContext().stop(getSelf());
                }
            }
        };
    }

    private void send(ActorRef connection) {
        File file=new File("/media/lq/Data/CloudData/Dropbox/distml/DistML/src/main/java/com/intel/distml/model/demo_tao/TestFile");
        Long length = file.length();
        System.out.println("File length: " + length);
        byte[] content = new byte[length.intValue()];
        try {
            FileInputStream in = new FileInputStream(file);
            in.read(content);
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        ByteStringBuilder bsb = new ByteStringBuilder();
        bsb.putBytes(content);
        if (useTCP)
            connection.tell(TcpMessage.write(bsb.result()),getSelf());
           // connection.tell(TcpMessage.write(bsb.result(), 0), getSelf());
        else {
            ActorSelection server = getContext().actorSelection("akka.tcp://ServerSystem@localhost:1234/user/Server");
            server.tell(content, getSelf());
        }
    }

    public void run() {
        final String config = ""
                + "akka.loglevel = \"INFO\"\n"
                + "akka.stdout-loglevel = \"INFO\"\n"
                + "akka.actor.provider = \"akka.remote.RemoteActorRefProvider\"\n"
                + "akka.remote.netty.tcp.port = 0\n"
                + "akka.remote.netty.tcp.maximum-frame-size = 1024000000b";
        ActorSystem clientSystem = ActorSystem.create("ClientSystem", ConfigFactory.parseString(config));
        ActorRef client = clientSystem.actorOf(Props.create(Client.class), "Client");
        clientSystem.awaitTermination();
    }
}