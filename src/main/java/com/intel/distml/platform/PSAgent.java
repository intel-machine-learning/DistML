package com.intel.distml.platform;

import com.intel.distml.api.DMatrix;
import com.intel.distml.api.Model;
import com.intel.distml.util.DataStore;
import com.intel.distml.util.KeyCollection;
import com.intel.distml.util.Logger;
import com.intel.distml.util.Utils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.LinkedList;

/**
 * Created by yunlong on 1/18/16.
 */
public class PSAgent extends Thread {

    String host;
    ServerSocket ss;
    boolean running = false;

    Model model;
    HashMap<String, DataStore> stores;

    LinkedList<FetchService> clients = new LinkedList<FetchService>();

    public PSAgent(Model model, HashMap<String, DataStore> stores, String psNetwordPrefix) {
        this.model = model;
        this.stores = stores;

        try {
            host = Utils.getLocalIP(psNetwordPrefix);
            ss = new ServerSocket(0);
            ss.setSoTimeout(1000000);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String addr() {
        return host + ":" + ss.getLocalPort();
    }

    public void disconnect() {
        running = false;

        try {
            ss.close();

            for (FetchService s : clients) {
                s.socket.close();
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public DataBusProtocol.DistMLMessage handle(DataBusProtocol.DistMLMessage msg) {
        log("handle: " + msg);
        if (msg instanceof DataBusProtocol.FetchRequest) {// Fetch parameters
            DataBusProtocol.FetchRequest req = (DataBusProtocol.FetchRequest)msg;

            DMatrix m = model.getMatrix(req.matrixName);
            KeyCollection rows = req.rows;
            KeyCollection cols = req.cols;

            DataStore store = stores.get(req.matrixName);

            log("partial data request received: " + req.matrixName + ", " + req.rows + ", " + rows);

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

    public void run() {
        running = true;

        while (running) {
            try {
                Socket s = ss.accept();
                log("worker connected: " + s.getInetAddress());
                FetchService c = new FetchService(s);
                clients.add(c);
                c.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    class FetchService extends Thread {

        Socket socket;
        DataInputStream is;
        DataOutputStream os;

        public FetchService(Socket socket) {
            try {
                this.socket = socket;
                this.is = new DataInputStream(socket.getInputStream());
                this.os = new DataOutputStream(socket.getOutputStream());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void run() {
            try {
                while(true) {
                    log("reading...");
                    int reqSize = is.readInt();
                    log("request size: " + reqSize);
                    DataBusProtocol.DistMLMessage req = DataBusProtocol.DistMLMessage.readDistMLMessage(is, model);
                    DataBusProtocol.DistMLMessage res = handle(req);

                    int len = res.sizeAsBytes(model);
                    log("write len: " + len);
                    os.writeInt(len);
                    res.write(os, model);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void log(String msg) {
//        Logger.info(msg, "PSAgent");
    }
}
