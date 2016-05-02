package com.intel.distml.platform;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import akka.io.TcpMessage;
import akka.pattern.Patterns;
import akka.util.ByteStringBuilder;
import com.intel.distml.api.DMatrix;
import com.intel.distml.api.DataBus;
import com.intel.distml.api.Model;
import com.intel.distml.util.*;
import scala.concurrent.Await;
import scala.concurrent.Future;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.LinkedList;

/**
 * Created by yunlong on 1/16/16.
 */

public class WorkerAgent implements DataBus  {

    static String MODUAL = "DataBus";

    Model model;
    Socket[] servers;

    Channel[] channels;

    public WorkerAgent(Model model, String[] psInfo) {
        this.model = model;

        servers = new Socket[psInfo.length];
        channels = new Channel[psInfo.length];
        try {
            for (int i = 0; i < psInfo.length; i++) {
                System.out.println("connect to " + psInfo[i]);
                String[] info = psInfo[i].split(":");
                servers[i] = new Socket(info[0], Integer.parseInt(info[1]));
                servers[i].setSoTimeout(1000000);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
            throw new IllegalStateException("failed to connect to server");
        }
    }

    public void psAvailable(int index, String addr) {
        try {
            System.out.println("connect to " + addr);
            String[] info = addr.split(":");
            servers[index] = new Socket(info[0], Integer.parseInt(info[1]));

            synchronized (channels) {
                if (channels[index] != null) {
                    channels[index].init(servers[index]);
                }
            }
        }
        catch (IOException e) {
            e.printStackTrace();
            servers[index] = null;
            throw new IllegalStateException("failed to connect to server");
        }
    }


    public byte[][] fetch(String matrixName, KeyCollection rowKeys, DataDesc format) {
        log("fetch: " + matrixName + ", " + rowKeys);

        DMatrix m = model.getMatrix(matrixName);
        KeyCollection[] partitions = m.partitions;

        int reqCount = 0;
        synchronized (channels) {
            for (int i = 0; i < partitions.length; ++i) {
                KeyCollection keys = partitions[i].intersect(rowKeys);
                log("request keys: " + keys);
                if (!keys.isEmpty()) {
                    DataBusProtocol.FetchRequest req = new DataBusProtocol.FetchRequest(matrixName, keys, KeyCollection.ALL);
                    Channel c = new Channel(i, servers[i], model, req);
                    channels[i] = c;
                    c.start();
                    reqCount++;
                }
            }
        }

        for (Channel c : channels) {
            if (c == null) continue;
            try {
                c.join();
            } catch (InterruptedException e) {}
        }

        try {
            byte[][] data = new byte[reqCount][];
            int index = 0;
            for (Channel c : channels) {
                if (c == null) continue;
                data[index] = ((DataBusProtocol.FetchResponse) c.result).data;
                index++;
            }

            synchronized (channels) {
                for (int i = 0; i < channels.length; i++)
                    channels[i] = null;
            }

            return data;
        } catch (Exception e) {
            e.printStackTrace();
            Logger.error(e.toString(), MODUAL);
            throw new RuntimeException("no result.");
        }
    }

    public void push(String matrixName, DataDesc format, byte[][] data) {

        DMatrix m = model.getMatrix(matrixName);
        KeyCollection[] partitions = m.partitions;
        //DataDesc format = m.getFormat();

        int reqCount = 0;
        synchronized (channels) {
            for (int i = 0; i < partitions.length; ++i) {
                byte[] d = data[i];
                if (d != null) {
                    DataBusProtocol.PushRequest req = new DataBusProtocol.PushRequest(matrixName, format, d);
                    Channel c = new Channel(i, servers[i], model, req);
                    channels[i] = c;
                    c.start();
                    reqCount++;
                }
            }
        }
        for (Channel c : channels) {
            if (c == null) continue;
            try {
                c.join();
            } catch (InterruptedException e) {}
        }

        try {
            for (Channel c : channels) {
                if (c == null) continue;
                if (!((DataBusProtocol.PushResponse)c.result).success) {
                    Logger.error("failed to push data", MODUAL);
                    break;
                }
            }

            synchronized (channels) {
                for (int i = 0; i < channels.length; i++)
                    channels[i] = null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            Logger.error(e.toString(), MODUAL);
            throw new RuntimeException("no result.");
        }
    }

    public void disconnect() {
        DataBusProtocol.CloseRequest req = new DataBusProtocol.CloseRequest();
        for (Socket s : servers) {
            try {
                DefaultDataWriter dos = new DefaultDataWriter(new DataOutputStream(s.getOutputStream()));
                dos.writeInt(req.sizeAsBytes(model));
                req.write(dos, model);
                dos.flush();
                s.getOutputStream().flush();
                s.close();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            log("connection closed: " + s.getRemoteSocketAddress());
        }
    }


    static class Channel extends Thread {

        int STATE_RUNNING = 0;
        int STATE_FINISH  = 1;
        int STATE_WAITING = 2;

        int state;

        Socket socket;
        AbstractDataReader is;
        AbstractDataWriter os;

        int index;
        Model model;
        DataBusProtocol.DistMLMessage msg;

        int resultSize;
        DataBusProtocol.DistMLMessage result;

       public Channel(int index, Socket socket, Model model, DataBusProtocol.DistMLMessage msg) {
            this.index = index;
            this.model = model;
            this.msg = msg;
            state = STATE_WAITING;

            init(socket);
        }

        public void init(Socket newSocket) {
            log("init socket: " + newSocket.getRemoteSocketAddress());

            if (state == STATE_RUNNING) {
                log("closing old socket");
                try {
                    socket.close();
                } catch (IOException e) {}

                while (state != STATE_WAITING) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {}
                }
            }

            log("try to use new socket");
            socket = newSocket;
            try {
                this.is = new DefaultDataReader(new DataInputStream(socket.getInputStream()));
                this.os = new DefaultDataWriter(new DataOutputStream(socket.getOutputStream()));

                System.out.println("change state to running now");
                state = STATE_RUNNING;
            } catch (Exception e) {
                e.printStackTrace();
                state = STATE_WAITING;
            }
        }

        @Override
        public void run() {
            state = STATE_RUNNING;

            while(state != STATE_FINISH) {
                while(state == STATE_WAITING) {
                    try {
                        Thread.sleep(1000);
                    }
                    catch (InterruptedException e) {}
                }
                try {
                    int len = msg.sizeAsBytes(model);

                    log("write len: " + len);
                    os.writeInt(len);
                    msg.write(os, model);

                    is.waitUntil(4);
                    resultSize = is.readInt();
                    log("result size: " + resultSize);

                    result = DataBusProtocol.DistMLMessage.readDistMLMessage(is, model);
                    state = STATE_FINISH;

                } catch (Exception e) {
                    System.out.println("failed to communicate with server, wait ps available again");
                    e.printStackTrace();
                    result = null;
                    state = STATE_WAITING;
                }
            }
        }

    }

    private static void log(String msg) {
        Logger.info(msg, MODUAL);
    }
}
