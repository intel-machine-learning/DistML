package com.intel.distml.model.demo_echo;

/**
 * Created by lq on 6/22/15.
 */
import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.io.Tcp;
import akka.io.Tcp.ConnectionClosed;
import akka.io.Tcp.Received;
import akka.io.Tcp.Write;
import akka.io.TcpMessage;
import akka.util.ByteString;
import akka.util.ByteStringBuilder;
import io.netty.buffer.ByteBuf;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

public class Echo_server extends UntypedActor {
    public Echo_server( ) {
        TcpMessage.bind(getSelf(),
                new InetSocketAddress("localhost", 1234), 100);
    }

    @Override
    public void onReceive(Object msg) throws Exception {
        System.out.println("Echo_server received:" + msg);
        if  (msg instanceof Tcp.Bind) {
            System.out.println("Server bound");
            getSender().tell(TcpMessage.register(getSelf()), getSelf());

        }else if (msg instanceof Tcp.Received) {
            final ByteString data = ((Received) msg).data();
            getSender().tell(TcpMessage.write(data), getSelf());
        }  else if (msg instanceof Tcp.Connected) {
            getSender().tell(TcpMessage.register(getSelf()), getSelf());
        }

        else if (msg instanceof ConnectionClosed) {
            getContext().stop(getSelf());
        }
        else if (msg instanceof Tcp.Write)  {
            System.out.println(msg);
//            ByteBuffer msg2 = (ByteBuffer) msg.data();
//            msg2.flip();
//            byte[] data = new byte[msg2.remaining()];
//            ByteStringBuilder bsb = new ByteStringBuilder();
//            bsb.putBytes(data);
//            //final ByteString data = ((Received) msg2).data();
//            getSender().tell(TcpMessage.write(bsb.result()), getSelf());
        }
        else{
            ByteBuf result = (ByteBuf) msg;
            byte[] result1 = new byte[result.readableBytes()];
            // msg中存储的是ByteBuf类型的数据，把数据读取到byte[]中
            result.readBytes(result1);
            String resultStr = new String(result1);
            // 接收并打印客户端的信息
            System.out.println("Client said:" + resultStr);
        }
    }

}