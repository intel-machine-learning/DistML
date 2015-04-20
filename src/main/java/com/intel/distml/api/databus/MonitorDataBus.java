package com.intel.distml.api.databus;

/**
 * Created by yunlong on 2/2/15.
 */
public interface MonitorDataBus {

    public void broadcast(String name, Object value);

}
