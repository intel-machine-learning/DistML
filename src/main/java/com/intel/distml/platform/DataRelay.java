package com.intel.distml.platform;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.io.Tcp;
import akka.io.TcpMessage;
import akka.japi.Creator;
import akka.util.ByteString;
import akka.util.ByteStringBuilder;
import com.intel.distml.util.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteOrder;

/**
 * Created by taotaotheripper on 2015/5/8.
 */

public class DataRelay extends UntypedActor {

    public static class CloseAtOnce {

        CloseAtOnce() {
        }
    }

    public static class Disconnect {
        int id;
        public Disconnect(int id) {
            this.id = id;
        }
    }

    private static final int SERIALIZER_BUF_MIN = 4096000;      // 4M
    private static final int SERIALIZER_BUF_MAX = 409600000;      // 400 M

    private int id;     // used for disconnect in parameter server side
    private ActorRef connection;
    private ActorRef responsor;
    private ActorRef connectionListener;

    private DataSerializer serializer;
    int reqSize;
    ByteStringBuilder buf;

    public DataRelay(int id, ActorRef connection, ActorRef responsor) {
        this.id = id;
        this.connection = connection;

        serializer = new DataSerializer(SERIALIZER_BUF_MIN,SERIALIZER_BUF_MAX);
        this.responsor = responsor;

        buf = new ByteStringBuilder();
        reqSize = -1;
    }

    public DataRelay(ActorRef connectionListener) {
        this.connectionListener = connectionListener;
        this.connection = null;
        this.responsor = null;

        serializer = new DataSerializer(SERIALIZER_BUF_MIN,SERIALIZER_BUF_MAX);

        buf = new ByteStringBuilder();
        reqSize = -1;
    }

    public static Props props(final int id, final ActorRef connection, final ActorRef responsor) {
        return Props.create(new Creator<DataRelay>() {
            private static final long serialVersionUID = 1L;
            public DataRelay create() throws Exception {
                return new DataRelay(id, connection, responsor);
            }
        });
    }

    public static Props props(final ActorRef connectionListener) {
        return Props.create(new Creator<DataRelay>() {
            private static final long serialVersionUID = 1L;
            public DataRelay create() throws Exception {
                return new DataRelay(connectionListener);
            }
        });
    }

    @Override
    public void onReceive(Object msg) throws Exception {
        if (msg instanceof Tcp.CommandFailed) {
            //log("Connection failed.");
            getContext().stop(getSelf());
        } else if (msg instanceof Tcp.Connected) {
            //log("Connected: " + getSender());
            connection = getSender();
            getSender().tell(TcpMessage.register(getSelf()), getSelf());
            connectionListener.tell(msg, getSelf());
        } else if (msg instanceof Tcp.ConnectionClosed) {
            log("Connection closed.");
            serializer = null;
            buf = null;
            responsor.tell(new Disconnect(id), getSelf());
        } else if (msg instanceof Disconnect) {
            log("closing connection.");
            disconnect();
        } else if (msg instanceof Tcp.Received) {
            ByteString data = ((Tcp.Received) msg).data();

            if (reqSize == -1) {
                if (data.length() >= 4) {
                    reqSize = data.iterator().getInt(ByteOrder.BIG_ENDIAN);
                }
            }
            buf.append(data);
            checkBufComplete();
        } else if (msg instanceof DataBusProtocol.ScamlMessage) {
            log("onReceive: " + msg);
            responsor = getSender();
            sendRequest(msg);
        } else {
            System.out.println("other message, relay it: " + msg.getClass());
            responsor = getSender();
            sendRequest(msg);
        }
    }

    private void checkBufComplete() {
        //log("check buffer complete: " + buf.length() + ", " + reqSize);
        if (reqSize == -1) {
            return;
        }

        if (buf.length() < reqSize + 4) {
            return;
        }

        InputStream is = buf.result().iterator().asInputStream();
        try {
            is.read(new byte[4]);
        } catch (IOException e) {}

        //log("deserialize: " + reqSize);
        Object obj = serializer.deserialize(is);
        buf.clear();
        reqSize = -1;

        //log("notify responser ");
        responsor.tell(obj, getSelf());
    }

    private int sendRequest(Object request) {
        try {
            byte[] reqBuf = serializer.serialize(request);
            log("serialized data: " + request + ", " + reqBuf.length);

            ByteStringBuilder bsb2 = new ByteStringBuilder();
            bsb2.putInt(reqBuf.length, ByteOrder.BIG_ENDIAN);
            connection.tell(TcpMessage.write(bsb2.result()), getSelf());

            ByteStringBuilder bsb = new ByteStringBuilder();
            bsb.putBytes(reqBuf);

            connection.tell(TcpMessage.write(bsb.result()), getSelf());
//            connection.tell(TcpMessage.write(ByteString.fromArray(reqBuf)), getSelf());
            //log("sent");

            return reqBuf.length;
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }
    }

    private void disconnect() {
        connection.tell(TcpMessage.close(), getSelf());
    }

    private void log(String msg) {
        Logger.debug(msg, "DataRelaye-" + responsor);
    }
}