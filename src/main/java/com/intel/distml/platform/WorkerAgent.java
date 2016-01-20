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

    public WorkerAgent(Model model, String[] psInfo) {
        this.model = model;

        servers = new Socket[psInfo.length];
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

    public byte[][] fetch(String matrixName, KeyCollection rowKeys, DataDesc format) {
        log("fetch: " + matrixName + ", " + rowKeys);

        DMatrix m = model.getMatrix(matrixName);
        KeyCollection[] partitions = m.partitions;
        //DataDesc format = m.getFormat();

        LinkedList<Channel> reqs = new LinkedList<Channel>();
        for (int i = 0; i < partitions.length; ++i) {
            KeyCollection keys = partitions[i].intersect(rowKeys);
            log("request keys: " + keys);
            if (!keys.isEmpty()) {
                DataBusProtocol.FetchRequest req = new DataBusProtocol.FetchRequest(matrixName, keys, KeyCollection.ALL);
                Channel c = new Channel(servers[i], model, req);
                reqs.add(c);
                c.start();
            }
        }

        for (Channel c : reqs) {
            try {
                c.join();
            } catch (InterruptedException e) {}
        }

        try {
            byte[][] data = new byte[reqs.size()][];
            int index = 0;
            for (Channel c : reqs) {
                data[index] = ((DataBusProtocol.FetchResponse) c.result).data;
                index++;
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

        LinkedList<Channel> reqs = new LinkedList<Channel>();
        for (int i = 0; i < partitions.length; ++i) {
            byte[] d = data[i];
            if (d != null) {
                DataBusProtocol.PushRequest req = new DataBusProtocol.PushRequest(matrixName, format, d);
                Channel c = new Channel(servers[i], model, req);
                reqs.add(c);
                c.start();
            }
        }

        for (Channel c : reqs) {
            try {
                c.join();
            } catch (InterruptedException e) {}
        }

        try {
            for (Channel c : reqs) {
                if (!((DataBusProtocol.PushResponse)c.result).success) {
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            Logger.error(e.toString(), MODUAL);
            throw new RuntimeException("no result.");
        }
    }

    public void disconnect() {
        for (Socket s : servers) {
            try {
                s.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    static class Channel extends Thread {
        DataInputStream is;
        DataOutputStream os;

        Model model;
        DataBusProtocol.DistMLMessage msg;

        int resultSize;
        DataBusProtocol.DistMLMessage result;


        public Channel(Socket socket, Model model, DataBusProtocol.DistMLMessage msg) {

            try {
                this.is = new DataInputStream(socket.getInputStream());
                this.os = new DataOutputStream(socket.getOutputStream());
                this.model = model;
                this.msg = msg;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            try {
                int len = msg.sizeAsBytes(model);

                log("write len: " + len);
                os.writeInt(len);
                msg.write(os, model);

                Utils.waitUntil(is, 4);
                resultSize = is.readInt();
                log("result size: " + resultSize);

                result = DataBusProtocol.DistMLMessage.readDistMLMessage(is, model);
            } catch (Exception e) {
                e.printStackTrace();
                result = null;
            }
        }

    }

    private static void log(String msg) {
//        Logger.info(msg, MODUAL);
    }
}
