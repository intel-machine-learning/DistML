package com.intel.distml.platform;

import akka.actor.*;

import com.intel.distml.api.*;
import com.intel.distml.api.DataBus;
import com.intel.distml.util.KeyCollection;

import java.util.HashMap;

public class DataBusImpl<T> extends GeneralDataBus implements DataBus {

    ActorRef[] parameterServers;
    int parameterServerNumber;

    public DataBusImpl(ActorRef[] parameterServers, Model model, ActorContext context) {
        super(model, context);
        this.parameterServers = parameterServers;
        this.parameterServerNumber = parameterServers.length;
    }

    public <T> HashMap<Long, T> fetch(String matrixName, KeyCollection rowKeys, KeyCollection colsKeys) {
        DMatrix m = model.getMatrix(matrixName);
        return fetchFromRemote2(matrixName, rowKeys, colsKeys,  m.partitions, parameterServers);
    }

    public <T> void push(String matrixName, HashMap<Long, T> update) {
        DMatrix m = model.dataMap.get(matrixName);
        pushToRemote(matrixName, false, update, m.partitions, parameterServers);
    }

    public <T> void push(String matrixName, T[] update) {
        DMatrix m = model.dataMap.get(matrixName);
        pushToRemote(matrixName, update, m.partitions, parameterServers);
    }
}

