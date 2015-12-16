package com.intel.distml.platform;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import akka.pattern.Patterns;
import com.intel.distml.api.Model;
import com.intel.distml.util.Constants;
import com.intel.distml.util.KeyCollection;
import com.intel.distml.util.Logger;
import scala.concurrent.Await;
import scala.concurrent.Future;

import java.util.HashMap;
import java.util.LinkedList;

import static akka.dispatch.Futures.sequence;

/**
 * Created by taotao on 15-2-3.
 */
public class GeneralDataBus {

    static String MODUAL = "DataBus";

    Model model;
    ActorContext context;

    public GeneralDataBus(Model model, ActorContext context) {
        this.model = model;
        this.context = context;
    }

    public <T> HashMap<Long, T> fetchFromRemote2(String matrixName, KeyCollection rowKeys, KeyCollection colsKeys,
                                  KeyCollection[] partitions, ActorRef[] remotes) {
        log("fetch: " + matrixName + ", " + rowKeys);

        LinkedList<Future<Object>> requests = new LinkedList<Future<Object>>();
        for (int i = 0; i < partitions.length; ++i) {
            KeyCollection keys = partitions[i].intersect(rowKeys);
            log("request keys: " + keys);
            if (!keys.isEmpty()) {
                DataBusProtocol.FetchRawDataRequest req = new DataBusProtocol.FetchRawDataRequest(matrixName, keys, colsKeys);
                requests.add(Patterns.ask(remotes[i], req, Constants.DATA_FUTURE_TIMEOUT));
            }
        }
        Future<Iterable<Object>> responsesFuture = sequence(requests, context.dispatcher());

        try {
            Iterable<Object> responses = Await.result(responsesFuture, Constants.DATA_FUTURE_TIMEOUT_DURATION);
            HashMap<Long, T> data = new HashMap<Long, T>();
            for (Object response : responses) {
                HashMap<Long, T> tmp = (HashMap<Long, T>) response;
                data.putAll(tmp);
            }

            return data;
        } catch (Exception e) {
            e.printStackTrace();
            Logger.error(e.toString(), MODUAL);
            throw new RuntimeException("no result.");
            //return null; // TODO Return null?
        }
    }

    public <T> boolean pushToRemote(String matrixName, boolean initializeOnly, HashMap<Long, T> data, KeyCollection[] partitions, ActorRef[] remotes) {

        LinkedList<Future<Object>> responseFutures = new LinkedList<Future<Object>>();

        for (int serverIndex = 0; serverIndex < partitions.length; ++serverIndex) {
            KeyCollection p = partitions[serverIndex];
            HashMap<Long, T> m = new HashMap<Long, T>();
            for (long key : data.keySet()) {
                if (p.contains(key))
                    m.put(key, data.get(key));
            }
            if (m.size() > 0) {
                DataBusProtocol.PushUpdateRequest pushRequest = new DataBusProtocol.PushUpdateRequest(matrixName, m);
                responseFutures.add(Patterns.ask(remotes[serverIndex], pushRequest, Constants.DATA_FUTURE_TIMEOUT));
            }
        }

        Future<Iterable<Object>> responsesFuture = sequence(responseFutures, context.dispatcher());

        try {
            Iterable<Object> responses = Await.result(responsesFuture, Constants.DATA_FUTURE_TIMEOUT_DURATION);
            //System.out.println("response: " + responses);
            boolean result = true;
            for(Object res : responses) {
                if (!((DataBusProtocol.PushDataResponse)res).success) {
                    return false;
                }
            }
            log("push done");
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            Logger.error(e.toString(), MODUAL);
            throw new RuntimeException("failed to push.");
            //return null; // TODO Return null?
        }
    }

    public <T> boolean pushToRemote(String matrixName, T[] data, KeyCollection[] partitions, ActorRef[] remotes) {

        LinkedList<Future<Object>> responseFutures = new LinkedList<Future<Object>>();

        for (int serverIndex = 0; serverIndex < partitions.length; ++serverIndex) {
            KeyCollection p = partitions[serverIndex];
            HashMap<Long, T> m = new HashMap<Long, T>();
            for (int k = 0; k < data.length; k++) {
                if (p.contains(k))
                    m.put(new Long(k), data[k]);
            }
            if (m.size() > 0) {
                DataBusProtocol.PushUpdateRequest pushRequest = new DataBusProtocol.PushUpdateRequest(matrixName, m);
                responseFutures.add(Patterns.ask(remotes[serverIndex], pushRequest, Constants.DATA_FUTURE_TIMEOUT));
            }
        }

        Future<Iterable<Object>> responsesFuture = sequence(responseFutures, context.dispatcher());

        try {
            Iterable<Object> responses = Await.result(responsesFuture, Constants.DATA_FUTURE_TIMEOUT_DURATION);
            //System.out.println("response: " + responses);
            boolean result = true;
            for(Object res : responses) {
                if (!((DataBusProtocol.PushDataResponse)res).success) {
                    return false;
                }
            }
            log("push done");
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            Logger.error(e.toString(), MODUAL);
            throw new RuntimeException("failed to push.");
            //return null; // TODO Return null?
        }
    }

    private void log(String msg) {
        Logger.info(msg, MODUAL);
    }
}
