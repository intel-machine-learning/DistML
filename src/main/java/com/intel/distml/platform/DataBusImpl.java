package com.intel.distml.platform;

import akka.actor.*;

import com.intel.distml.api.*;
import com.intel.distml.api.databus.DataBus;

public class DataBusImpl<T> extends ServerDataBusImpl implements DataBus {

    public DataBusImpl( ActorRef[] parameterServers, Model model, ActorContext context) {
        super(parameterServers, model, context);
    }

}

