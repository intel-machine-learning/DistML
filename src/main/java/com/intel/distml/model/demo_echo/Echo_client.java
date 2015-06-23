package com.intel.distml.model.demo_echo;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.io.Tcp;
import akka.io.Tcp.Bound;
import akka.io.Tcp.Write;
import akka.io.Tcp.CommandFailed;
import akka.io.Tcp.Connected;
import akka.io.TcpMessage;
import akka.util.ByteString;
import akka.util.ByteStringBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * Created by lq on 6/22/15.
 */
public class Echo_client extends UntypedActor {
    private final ActorRef tcpManager;
    private final InetSocketAddress server_address;

    public Echo_client(ActorRef tcpManager) {
        this.tcpManager = tcpManager;
        this.server_address = new InetSocketAddress("localhost", 1234);
    }

    @Override
    public void onReceive(Object msg) throws Exception {
        System.out.println("Echo_client received:" + msg);
        if (msg instanceof Integer) {
            final int port = (Integer) msg;
            //    final InetSocketAddress endpoint = new InetSocketAddress("localhost", port);
            //     final Object cmd = TcpMessage.bind(getSelf(), server_address, 100);
            //  tcpManager.tell(cmd, getSelf());
            getSender().tell(TcpMessage.bind(getSelf(), server_address, 100), getSelf());
        } else if (msg instanceof Bound) {
            getSender().tell(TcpMessage.connect(server_address), getSelf());
            //  tcpManager.tell(TcpMessage.connect(endpoint), getSelf());
            //    tcpManager.tell(msg, getSelf());
        } else if (msg instanceof CommandFailed) {
            getContext().stop(getSelf());
        } else if (msg instanceof Tcp.Register) {
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
            getSender().tell(TcpMessage.write(bsb.result()),getSelf());
           // getSender().tell(content, getSelf());

            BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
            for(;;) {
                String line = in.readLine();
                ByteString line2 = ByteString.fromString(line);
                if (line == null) {
                    return;
                } else {
                    getSender().tell(TcpMessage.write(line2), getSelf());
                }
            }
//            File file=new File("/media/lq/Data/CloudData/Dropbox/distml/DistML/src/main/java/com/intel/distml/model/demo_tao/TestFile");
//            Long length = file.length();
//            System.out.println("File length: " + length);
//            byte[] content = new byte[length.intValue()];
//            try {
//                FileInputStream in = new FileInputStream(file);
//                in.read(content);
//                in.close();
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//            ByteStringBuilder bsb = new ByteStringBuilder();
//            bsb.putBytes(content);
//           // getSender().tell(TcpMessage.write(bsb.result()),getSelf());
//            ActorSelection server = getContext().actorSelection("akka.tcp://ServerSystem@localhost:1234/user/Server");
//            server.tell(content, getSelf());
            // getSender().tell(TcpMessage.register(getSelf()), getSelf());
//            final ActorRef handler = getContext().actorOf(Props.create(Echo_server.class));
//            getSender().tell(TcpMessage.register(handler), getSelf());
        } else if (msg instanceof Tcp.Received) {
            System.out.println("Message received by tcp");
        }
    }

}
