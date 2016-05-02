package com.intel.distml.platform;

import akka.actor.ActorRef;
import com.intel.distml.api.DMatrix;
import com.intel.distml.api.Model;
import com.intel.distml.util.*;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Created by yunlong on 1/18/16.
 */
public class PSSync extends Thread {

    static final int ROLE_PRIMARY = 0;
    static final int ROLE_STANDBY = 1;

    static final int MAX_SYNC_BLOCK_SIZE = 1000000;
    static final int SYNC_INTERVAL = 10000;

    ActorRef owner;
    int role;

    boolean running = false;
    Socket socket;
    DataOutputStream dos;
    DataInputStream dis;

    Model model;
    HashMap<String, DataStore> stores;

    public PSSync(ActorRef owner, Model model, HashMap<String, DataStore> stores) {
        this.owner = owner;
        this.model = model;
        this.stores = stores;
    }

    public void asPrimary(Socket socket) {
        log("start PS sync as primary");
        this.socket = socket;
        role = ROLE_PRIMARY;
        try {
            dos = new DataOutputStream(socket.getOutputStream());
            dis = new DataInputStream(socket.getInputStream());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        start();
    }

    public void asStandBy(Socket socket) {
        log("start PS sync as standby");
        this.socket = socket;
        role = ROLE_STANDBY;

        log("connecting to primvary server");
        DataBusProtocol.SyncRequest req = new DataBusProtocol.SyncRequest();
        try {
            dos = new DataOutputStream(socket.getOutputStream());
            dis = new DataInputStream(socket.getInputStream());
            DefaultDataWriter out = new DefaultDataWriter(dos);
            out.writeInt(req.sizeAsBytes(model));
            req.write(out, model);
            dos.flush();
            log("request to sync");
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        start();
    }

    public void disconnect() {
        running = false;
        try {
            socket.close();
        }
        catch (Exception e) {
        }
    }

    @Override
    public void run() {
        running = true;

        try {
            if (role == ROLE_PRIMARY) {
                syncService();
            } else {
                syncClient();
            }
        } catch (Exception e) {
            if (running)
                e.printStackTrace();
        }
    }

    private void syncService() throws Exception {
        while(running) {
            log("new round of sync");
            for (String name : model.dataMap.keySet()) {
                byte[] nameBytes = name.getBytes();

                DataStore store = stores.get(name);
                int rows = (int) store.rows().size();
                int maxRowsToSync = MAX_SYNC_BLOCK_SIZE / store.rowSize();

                int startRow = 0;
                int leftRows = rows;
                while (leftRows > 0) {
                    int rowsToSync = (leftRows > maxRowsToSync) ? maxRowsToSync : leftRows;
                    int endRow = rowsToSync + startRow - 1;

                    log("sync: " + name + ", " + startRow + ", " + endRow);
                    dos.writeInt(nameBytes.length);
                    dos.write(nameBytes);
                    dos.writeInt(startRow);
                    dos.writeInt(endRow);

                    store.syncTo(dos, startRow, endRow);
                    startRow += rowsToSync;
                    leftRows -= rowsToSync;

                    try {
                        Thread.sleep(SYNC_INTERVAL);
                    } catch (InterruptedException e) {
                    }
                }
            }
        }
    }

    private void syncClient() throws Exception {
        while (running) {
            log("handling sync ...");
            Utils.waitUntil(dis, 4);
            int nameLen = dis.readInt();
            log("name len: " + nameLen);
            Utils.waitUntil(dis, nameLen);
            byte[] nameBytes = new byte[nameLen];
            dis.read(nameBytes);
            String name = new String(nameBytes);
            log("name: " + name);

            DataStore store = stores.get(name);
            int startRow = dis.readInt();
            int endRow = dis.readInt();
            log("rows: " + startRow + ", " + endRow);
            store.syncFrom(dis, startRow, endRow);
        }
    }

    private void log(String msg) {
        Logger.info(msg, "PSAgent");
    }
    private void warn(String msg) {
        Logger.warn(msg, "PSAgent");
    }
}
