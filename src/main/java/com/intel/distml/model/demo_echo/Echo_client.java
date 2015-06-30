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
import akka.util.ByteIterator;
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
            getSender().tell(TcpMessage.bind(getSelf(), server_address, 100), getSelf());
        } else if (msg instanceof Bound) {
            getSender().tell(TcpMessage.connect(server_address), getSelf());
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
        }
        else if (msg instanceof Tcp.Write) {
            Tcp.Write tmp = (Tcp.Write) msg;
            final ByteString s = tmp.data();
            ByteIterator it = s.iterator();
            byte[] a = new byte[s.length()];
            it.getBytes(a);
            String ss = new String(a);
            System.out.println(ss);
            getSender().tell(TcpMessage.write(s), getSelf());
        }else if (msg instanceof Tcp.Received) {
            System.out.println("Message received by tcp");
        }
    }

}
