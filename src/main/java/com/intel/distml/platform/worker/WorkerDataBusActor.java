package com.intel.distml.platform.worker;

import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.japi.Creator;
import com.intel.distml.api.Model;
import com.intel.distml.transport.DataBusProtocol;
import com.intel.distml.util.Logger;
import com.intel.distml.util.Matrix;

import java.util.Iterator;

/**
 * Warn: temporarily unused.
 *
 * Created by taotao on 15-1-14.
 */
class WorkerDataBusActor extends UntypedActor {

	private Iterator samples;
	private final Model model;
    private final int workerIndex;

    // Worker Lead State
    class InnerStateData {
        //boolean siblingsInitialized = false;
        int currentProgress = -1; // Initialized with a negative number
        Matrix currentSample = null;
    }
    private InnerStateData innerState = new InnerStateData();

	public static Props props(final int workerIndex, final Model model, final Iterator samples) {
		return Props.create(new Creator<WorkerDataBusActor>() {
			private static final long serialVersionUID = 1L;
			public WorkerDataBusActor create() throws Exception {
				return new WorkerDataBusActor(workerIndex, model, samples);
			}
		});
	}

	WorkerDataBusActor(final int workerIndex, final Model model, Iterator samples) {
        this.workerIndex = workerIndex;
		this.model = model;
        this.samples = samples;
	}

    @Override
	public void onReceive(Object msg) {
        if (msg instanceof DataBusProtocol.FetchDataRequest) { // Partitioned Data Request
            DataBusProtocol.FetchDataRequest req = (DataBusProtocol.FetchDataRequest)msg;

            Matrix responseData = null;
            if (msg instanceof DataBusProtocol.PartialDataRequest) { // Partial Data Request
                DataBusProtocol.PartialDataRequest preq = (DataBusProtocol.PartialDataRequest)msg;
                responseData = model.getMatrix(req.matrixName).subMatrix(preq.rows, preq.cols);

            } else { // Partitioned Data Request
                responseData = model.getMatrix(req.matrixName);
            }

            Logger.DebugLog("send back data: " + responseData, Logger.Role.DATABUS, 0);
            DataBusProtocol.Data data = new DataBusProtocol.Data(req.matrixName, responseData);
            getSender().tell(data, getSelf());
        } else if (msg instanceof DataBusProtocol.SampleRequest) {
            DataBusProtocol.SampleRequest req = (DataBusProtocol.SampleRequest)msg;
            DataBusProtocol.Data data = new DataBusProtocol.Data(Model.MATRIX_SAMPLE, model.getCache(Model.MATRIX_SAMPLE));
            getSender().tell(data, getSelf());
        } else unhandled(msg);
    }


    private void dataBusLog(String msg) {
        Logger.InfoLog(msg, Logger.Role.DATABUS, workerIndex);
    }
}
