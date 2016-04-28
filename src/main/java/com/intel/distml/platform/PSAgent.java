package com.intel.distml.platform;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import com.intel.distml.api.DMatrix;
import com.intel.distml.api.Model;
import com.intel.distml.util.*;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * Created by yunlong on 1/18/16.
 */
public class PSAgent extends Thread {

    class DataBuffer {
        int len = -1;
        byte[] data;

        ByteBuffer buf = ByteBuffer.allocate(4);

        ByteBuffer resBuf;

        void reset() {
            len = -1;
            buf = ByteBuffer.allocate(4);
        }

        boolean readData(SocketChannel channel) throws IOException {
            if (len == -1) {
                channel.read(buf);
//                log("read head: " + buf.position());
                if (buf.position() < 4) {
                    return false;
                }

                len = (new DataInputStream(new ByteArrayInputStream(buf.array()))).readInt();
                buf = ByteBuffer.allocate(len);
            }


            channel.read(buf);
            if (buf.position() == len) {
                data = buf.array();

                reset();
                return true;
            }

            return false;
        }

        boolean writeData(SocketChannel channel) throws IOException {
            if (resBuf == null) {
                //warn("invalid state: resbuf is null");
                return true;
            }

            log("attempt to write: " + resBuf.position() + ", " + resBuf.remaining());
            int len1 = resBuf.remaining();
            int len2 = channel.write(resBuf);
            log("write: " + len1 + ", " + len2 + ", " + resBuf.position() + ", " + resBuf.remaining());
            return len1 == len2;
        }
    }

    ActorRef owner;

    String hostName;
    String ip;
    boolean running = false;

    Model model;
    HashMap<String, DataStore> stores;

    ServerSocketChannel ss;
    Selector selector;

    HashMap<SocketChannel, DataBuffer> buffers = new HashMap<SocketChannel, DataBuffer>();


    public PSAgent(ActorRef owner, Model model, HashMap<String, DataStore> stores, String psNetwordPrefix) {
        this.owner = owner;
        this.model = model;
        this.stores = stores;

        try {
            String[] addr = Utils.getNetworkAddress(psNetwordPrefix);
            ip = addr[0];
            hostName = addr[1];

            selector = Selector.open();

            ss = ServerSocketChannel.open();
            ss.socket().bind(new InetSocketAddress(0));
            ss.configureBlocking(false);
            ss.register(selector, SelectionKey.OP_ACCEPT);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String addr() {
        return ip + ":" + ss.socket().getLocalPort();
    }

    public String hostName() {
        return hostName;
    }

    public void disconnect() {
        running = false;

        try {
            ss.socket().close();
            selector.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        closeClients();
    }

    public void closeClients() {

        for (SocketChannel c : buffers.keySet()) {
            try {
                c.socket().close();
                c.keyFor(selector).cancel();
            }
            catch (IOException e) {
            }
        }

        buffers.clear();
    }

    @Override
    public void run() {
        running = true;

        try {
            while (running) {
                selector.select();
                Iterator ite = selector.selectedKeys().iterator();
                while (ite.hasNext()) {
                    SelectionKey key = (SelectionKey) ite.next();
                    //log("check: " + key + ", " + key.interestOps());
                    if (key.isAcceptable()) {
                        SocketChannel channel = ss.accept();
                        if (channel != null) {
                            log("worker connected: " + channel.socket().getRemoteSocketAddress());
                            channel.configureBlocking(false);
                            buffers.put(channel, new DataBuffer());
                            channel.register(this.selector, SelectionKey.OP_READ);
                        }
                    } else if (key.isReadable()) {
                        read(key);
                    } else if (key.isWritable()) {
                        write(key);
                    }

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void read(SelectionKey key) throws Exception {
        SocketChannel channel = (SocketChannel) key.channel();
        DataBuffer buf = buffers.get(channel);
        if (buf.readData(channel)) {

            InputStream is = new ByteArrayInputStream(buf.data);
            DataInputStream dis = new DataInputStream(is);
            AbstractDataReader reader = new DefaultDataReader(dis);

            DataBusProtocol.DistMLMessage req = DataBusProtocol.DistMLMessage.readDistMLMessage(reader, model);
            if (req instanceof DataBusProtocol.CloseRequest) {// worker disconnected
                try {
                    key.cancel();
                    ((SocketChannel) key.channel()).socket().close();
                }
                catch (IOException e){}
                log("client disconnected: " + ((SocketChannel) key.channel()).getRemoteAddress());
            }
            else {
                DataBusProtocol.DistMLMessage res = handle(req);

                int len = res.sizeAsBytes(model);
//                ByteArrayOutputStream bdos = new ByteArrayOutputStream(len + 4);
//                DataOutputStream dos = new DataOutputStream(bdos);
//                dos.writeInt(len);
//                res.write(dos, model);

                buf.resBuf = ByteBuffer.allocate(len+4);
                AbstractDataWriter out = new ByteBufferDataWriter(buf.resBuf);
                out.writeInt(len);
                res.write(out, model);
                buf.resBuf.flip();

                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                //log("register with OP_WRITE: " + key + ", " + key.interestOps());
//                channel.socket().getOutputStream().write(bdos.toByteArray());
            }
        }
    }

    private void write(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        DataBuffer buf = buffers.get(channel);
        if (buf.writeData(channel)) {
            buf.resBuf = null;
            key.interestOps(key.interestOps() & (~SelectionKey.OP_WRITE));
            //log("unregister with OP_WRITE: " + key + ", " + key.interestOps());
        }
    }

    public DataBusProtocol.DistMLMessage handle(DataBusProtocol.DistMLMessage msg) {
        //log("handle: " + msg);
        if (msg instanceof DataBusProtocol.FetchRequest) {// Fetch parameters
            DataBusProtocol.FetchRequest req = (DataBusProtocol.FetchRequest)msg;

            DMatrix m = model.getMatrix(req.matrixName);
            KeyCollection rows = req.rows;
            KeyCollection cols = req.cols;

            DataStore store = stores.get(req.matrixName);

            log("partial data request received: " + req.matrixName + ", " + req.rows + ", " + rows);
            long totalMemory = Runtime.getRuntime().totalMemory();
            long freeMemory = Runtime.getRuntime().freeMemory();
            if ((double)freeMemory/totalMemory < 0.1) {
                //warn("memory too low: free=" + freeMemory + ", total=" + totalMemory);
                owner.tell(new PSActor.AgentMessage(freeMemory, totalMemory), null);
            }

            byte[] result = store.handleFetch(m.getFormat(), rows);
            DataBusProtocol.FetchResponse res = new DataBusProtocol.FetchResponse(req.matrixName, m.getFormat(), result);
            return res;

        } else if (msg instanceof DataBusProtocol.PushRequest) {
            log("update push request received: " + msg);
            DataBusProtocol.PushRequest req = (DataBusProtocol.PushRequest)msg;

            DMatrix m = model.getMatrix(req.matrixName);
            DataStore store = stores.get(req.matrixName);

            log("handle push: " + req.matrixName + ", " + m + ", " + store);

            synchronized (store) {
                store.handlePush(m.getFormat(), req.data);
            }
            return new DataBusProtocol.PushResponse(true);
        }

        return null;
    }

    private void log(String msg) {
        //Logger.info(msg, "PSAgent");
    }
    private void warn(String msg) {
        Logger.warn(msg, "PSAgent");
    }
}
