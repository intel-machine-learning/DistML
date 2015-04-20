package com.intel.distml.util;

import akka.util.Timeout;
import scala.concurrent.duration.FiniteDuration;

import java.util.concurrent.TimeUnit;

/**
 * Created by taotao on 15-1-30.
 */
public class Constants {

    // Data may be transferred for a long time, and we must ensure that data is successfully transferred,
    // so we set a much longer timeout for data future.
    public static final Long DATA_FUTURE_TIME = 1000000L; // Unit: ms, 1000s
    public static final Timeout DATA_FUTURE_TIMEOUT = new Timeout(DATA_FUTURE_TIME, TimeUnit.MILLISECONDS);
    public static final FiniteDuration DATA_FUTURE_TIMEOUT_DURATION = DATA_FUTURE_TIMEOUT.duration();

    // Stop should be done in a short period, so we set a shorter timeout for stop future.
    public static final Long STOP_FUTURE_TIME = 30000L; // Unit: ms, 30s
    public static final Timeout STOP_FUTURE_TIMEOUT = new Timeout(STOP_FUTURE_TIME, TimeUnit.MILLISECONDS);
    public static final FiniteDuration STOP_FUTURE_TIMEOUT_DURATION = STOP_FUTURE_TIMEOUT.duration();

}
