package com.intel.distml.api;

import com.intel.distml.api.databus.ServerDataBus;

/**
 *
 * Applications should implement interface to save model to file system or collect to memory
 * after training complete.
 *
 * Created by yunlong on 2/1/15.
 */
public interface ModelWriter {

    public void writeModel(Model model, ServerDataBus dataBus);

}
