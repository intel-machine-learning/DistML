package com.intel.distml.platform;

import akka.actor.*;

import akka.io.Tcp;
import akka.io.TcpMessage;
import akka.pattern.Patterns;
import com.intel.distml.api.*;
import com.intel.distml.api.DataBus;
import com.intel.distml.util.Constants;
import com.intel.distml.util.DataDesc;
import com.intel.distml.util.KeyCollection;
import com.intel.distml.util.Logger;
import scala.concurrent.Await;
import scala.concurrent.Future;

import java.util.HashMap;
import java.util.LinkedList;

import static akka.dispatch.Futures.sequence;

public class DataBusImpl<T> implements DataBus {

    static String MODUAL = "DataBus";

    Model model;
    ActorContext context;

    ActorRef[] parameterServers;
    int parameterServerNumber;

    public DataBusImpl(ActorRef[] parameterServers, Model model, ActorContext context) {
        this.model = model;
        this.context = context;
        this.parameterServers = parameterServers;
        this.parameterServerNumber = parameterServers.length;
    }

    public byte[][] fetch(String matrixName, KeyCollection rowKeys, DataDesc format) {
        DMatrix m = model.getMatrix(matrixName);
        return fetch(matrixName, rowKeys, m.partitions, parameterServers, format);
    }

    public void push(String matrixName, DataDesc format, byte[][] data) {
        push(matrixName, format, data, parameterServers);
    }

    public void disconnect() {
        for (ActorRef ps : parameterServers) {
            ps.tell(new DataRelay.Disconnect(0), null);
        }
    }

    private byte[][] fetch(String matrixName, KeyCollection rowKeys, KeyCollection[] partitions, ActorRef[] remotes, DataDesc format) {
        log("fetch: " + matrixName + ", " + rowKeys);

        LinkedList<Future<Object>> requests = new LinkedList<Future<Object>>();
        for (int i = 0; i < partitions.length; ++i) {
            KeyCollection keys = partitions[i].intersect(rowKeys);
            log("request keys: " + keys);
            if (!keys.isEmpty()) {
                DataBusProtocol.FetchRequest req = new DataBusProtocol.FetchRequest(matrixName, keys, KeyCollection.ALL);
                requests.add(Patterns.ask(remotes[i], req, Constants.DATA_FUTURE_TIMEOUT));
            }
        }
        Future<Iterable<Object>> responsesFuture = sequence(requests, context.dispatcher());

        try {
            Iterable<Object> responses = Await.result(responsesFuture, Constants.DATA_FUTURE_TIMEOUT_DURATION);
            byte[][] data = new byte[partitions.length][];
            int index = 0;
            for (Object response : responses) {
                System.out.println("result: " + response.getClass());
                data[index] = (byte[]) response;
                index++;
            }
            return data;
        } catch (Exception e) {
            e.printStackTrace();
            Logger.error(e.toString(), MODUAL);
            throw new RuntimeException("no result.");
            //return null; // TODO Return null?
        }
    }

    private boolean push(String matrixName, DataDesc format, byte[][] data, ActorRef[] remotes) {

        LinkedList<Future<Object>> responseFutures = new LinkedList<Future<Object>>();

        for (int serverIndex = 0; serverIndex < remotes.length; ++serverIndex) {
            byte[] d = data[serverIndex];
            if (d != null) {
                DataBusProtocol.PushRequest pushRequest = new DataBusProtocol.PushRequest(matrixName, format, d);
                responseFutures.add(Patterns.ask(remotes[serverIndex], pushRequest, Constants.DATA_FUTURE_TIMEOUT));
            }
        }

        Future<Iterable<Object>> responsesFuture = sequence(responseFutures, context.dispatcher());

        try {
            Iterable<Object> responses = Await.result(responsesFuture, Constants.DATA_FUTURE_TIMEOUT_DURATION);
            for(Object res : responses) {
                if (!((DataBusProtocol.PushResponse)res).success) {
                    return false;
                }
            }
            log("push done");
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            Logger.error(e.toString(), MODUAL);
            throw new RuntimeException("failed to push.");
        }
    }

    private void log(String msg) {
        Logger.info(msg, MODUAL);
    }
}

