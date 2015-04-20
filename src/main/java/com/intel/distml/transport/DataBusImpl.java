package com.intel.distml.transport;

import akka.actor.*;

import akka.dispatch.OnSuccess;
import akka.pattern.Patterns;
import com.intel.distml.api.*;
import com.intel.distml.util.*;
import scala.concurrent.Await;
import scala.concurrent.Future;

import java.io.Serializable;
import java.util.LinkedList;

import static akka.dispatch.Futures.sequence;

public class DataBusImpl<T> extends ServerDataBusImpl {
    
    public static class PushUpdateRequest implements Serializable {
        private static final long serialVersionUID = 1L;

        public final Matrix updates;
        public final int layerIndex;
        public PushUpdateRequest(Matrix updates, int layerIndex) {
            this.updates = updates;
            this.layerIndex = layerIndex;
        }
    }

	private int workerIndex;
    private ActorRef[] workerDataBusActors;

    public DataBusImpl(int workerIndex, ActorRef[] workerDataBusActors, ActorRef[] parameterServers, Model model,
                       ActorContext context) {
        super(workerIndex, parameterServers, model, context);
        this.workerIndex = workerIndex;
        this.workerDataBusActors = workerDataBusActors;
	}

    private void log(String msg) {
        Logger.DebugLog(msg, Logger.Role.DATABUS, workerIndex);
    }

    public Matrix fetchFromWorker(String matrixName, KeyCollection rowKeys, KeyCollection colsKeys) {
        PartitionInfo p = model.getMatrix(matrixName).workerPartitions();

        return fetchFromRemote(matrixName, rowKeys, colsKeys,  p, workerDataBusActors);
    }

    /**
     * Async input data fetching. Fetch input data from target worker.
     * The result is sent back by DataBus.Data.
     */
    public void asyncFetchInputData(int targetWorkerIndex, int currentProgress) {
        Future<Object> responseFuture = Patterns.ask(workerDataBusActors[targetWorkerIndex],
                new DataBusProtocol.SampleRequest(), Constants.DATA_FUTURE_TIMEOUT);
        Patterns.pipe(responseFuture, context.dispatcher()).to(context.self());
    }

    public void asyncPushUpdates() {
        LinkedList<Future<Object>> responseFutures = new LinkedList<Future<Object>>();
        for (int parameterServerIndex = 0; parameterServerIndex < parameterServerNumber; ++parameterServerIndex) {
            LinkedList<DataBusProtocol.Data> updateList = new LinkedList<DataBusProtocol.Data>();

            for (String matrixName: model.dataMap.keySet()) {
                DMatrix m = model.dataMap.get(matrixName);
                if (m.type != DMatrix.TYPE_UPDATE)
                    continue;

                System.out.println("push update: " + matrixName);

                PartitionInfo wp = m.workerPartitions();
                PartitionInfo sp = m.serverPartitions();
                Matrix update = m.localCache;

                if ((sp == null) || (sp.type != PartitionInfo.Type.PARTITIONED)) { // only one parameter server
                    updateList.add(new DataBusProtocol.Data(matrixName, update));
                }
                else {
                    KeyCollection neededRows = sp.getPartition(parameterServerIndex).keys;
                    Matrix subMatrix = update.subMatrix(neededRows, KeyCollection.ALL);
                    if (subMatrix != null)
                        updateList.add(new DataBusProtocol.Data(matrixName, subMatrix));
                }
            }
            if (!updateList.isEmpty())
                responseFutures.add(Patterns.ask(parameterServers[parameterServerIndex],
                        new DataBusProtocol.PushDataRequest(updateList), Constants.DATA_FUTURE_TIMEOUT));
        }
        Future<Iterable<Object>> responsesFuture = sequence(responseFutures, context.dispatcher());

        // Use a temporary final actor reference
        final ActorRef worker = context.self();
        // [Caution] Should never modify outside state in this callback!
        responsesFuture.onSuccess(new OnSuccess<Iterable<Object>>() {
            public void onSuccess(Iterable<Object> responses) throws Throwable {
                // Use worker itself to be the fake sender^M
                //worker.tell(new DataBusProtocol.UpdateDone(true), worker);
            }
        }, context.dispatcher());
    }

    public void pushUpdatesAndWait() {
        for (String matrixName: model.dataMap.keySet()) {
            DMatrix m = model.dataMap.get(matrixName);
            if (m.type != DMatrix.TYPE_UPDATE)
                continue;

            if (m.localCache != null)
                continue;

            System.out.println("push update: " + matrixName + ", " + m.localCache);
            pushToRemote(matrixName, false, m.localCache, m.serverPartitions(), parameterServers);
        }
    }

//    public Matrix fetchSample() {
//        Matrix s = fetchSampleFromWorker(workerDataBusActors[sampleWorkerIndex]);
//        if (s != null) {
//            return s;
//        }
//
//        while(sampleWorkerIndex < workerDataBusActors.length-1) {
//            sampleWorkerIndex++;
//            s = fetchSampleFromWorker(workerDataBusActors[sampleWorkerIndex]);
//            if (s != null) {
//                return s;
//            }
//        }
//
//        return null;
//    }

    public Matrix fetchSamples(int workerIndex) {

        DataBusProtocol.SampleRequest req = new DataBusProtocol.SampleRequest();
        Future<Object> responseFuture = Patterns.ask(workerDataBusActors[workerIndex], req, Constants.DATA_FUTURE_TIMEOUT);

        try {
            DataBusProtocol.Data responses = (DataBusProtocol.Data) Await.result(responseFuture, Constants.DATA_FUTURE_TIMEOUT_DURATION);

            return responses.data;
        } catch (Exception e) {
            e.printStackTrace();
            Logger.ErrorLog(e.toString(), Logger.Role.DATABUS, dataBusId);
            throw new RuntimeException("no result.");
        }
    }

    public void initParamsFromServer() {
        log("fetch params from server begin.");
        if (!model.autoFetchParams) {
            log("auto fetch disabled.");
            return;
        }

        for (String matrixName: model.dataMap.keySet()) {
            DMatrix m = model.dataMap.get(matrixName);
            if (m.type != DMatrix.TYPE_PARAM)
                continue;

            PartitionInfo sp = m.serverPartitions();
            PartitionInfo wp = m.workerPartitions();

            if ((wp == null) || (wp.type == PartitionInfo.Type.COPIED)) {
                Matrix params = fetchFromRemote(matrixName, KeyCollection.ALL, KeyCollection.ALL, sp, parameterServers);
                m.setLocalCache(params);
            } else {
                Partition p = wp.getPartition(workerIndex);
                Matrix params = fetchFromRemote(matrixName, p.keys, KeyCollection.ALL, sp, parameterServers);
                log("param fetched: " + params);
                m.setLocalCache(params);
            }
        }
        log("fetch params from server done.");
    }
}

