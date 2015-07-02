package com.intel.distml.platform;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import com.intel.distml.api.Model;
import com.intel.distml.api.PartitionInfo;
import com.intel.distml.api.databus.ServerDataBus;
import com.intel.distml.api.DMatrix;
import com.intel.distml.util.KeyCollection;
import com.intel.distml.util.Logger;
import com.intel.distml.util.Matrix;

/**
 * Created by yunlong on 2/9/15.
 */
public class ServerDataBusImpl extends GeneralDataBus implements ServerDataBus {

    ActorRef[] parameterServers;
    int parameterServerNumber;

    public ServerDataBusImpl(ActorRef[] parameterServers, Model model, ActorContext context) {
        super(model, context);
        this.parameterServers = parameterServers;
        this.parameterServerNumber = parameterServers.length;
    }

    public Matrix fetchFromServer(String matrixName) {
        return fetchFromServer(matrixName, KeyCollection.ALL, KeyCollection.ALL);
    }

    public Matrix fetchFromServer(String matrixName, KeyCollection rowKeys) {
        return fetchFromServer(matrixName, rowKeys, KeyCollection.ALL);
    }

    public Matrix fetchFromServer(String matrixName, KeyCollection rowKeys, KeyCollection colsKeys) {
        Logger.DebugLog("fetch from server: " + matrixName + ", " + rowKeys, Logger.Role.DATABUS, 0);
        DMatrix matrix = model.getMatrix(matrixName);
        if (matrix == null) {
            return null;
        }
        PartitionInfo p = matrix.serverPartitions();
        return fetchFromRemote(matrixName, rowKeys, colsKeys,  p, parameterServers);
    }

    public void pushUpdate(String matrixName, Matrix update) {
        DMatrix m = model.dataMap.get(matrixName);
        pushToRemote(matrixName, false, update, m.serverPartitions(), parameterServers);
    }

    public void pushInitialParams(String matrixName, Matrix update) {
        DMatrix m = model.dataMap.get(matrixName);
        pushToRemote(matrixName, true, update, m.serverPartitions(), parameterServers);
    }
}
