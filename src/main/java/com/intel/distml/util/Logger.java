package com.intel.distml.util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by taotao on 15-1-14.
 *
 * Simple log system.
 */
public class Logger {

    private final static String[] levels = {"NOTUSED", "DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"};

    private final static int DEBUG      = 1;
    private final static int INFO       = 2;
    private final static int WARN       = 3;
    private final static int ERROR      = 4;
    private final static int CRITICAL   = 5;

    private static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss.SSS");

    private static int outputLevel = INFO;

    public static void Log(int level, String msg, String module) {
        if (level < outputLevel) return;

        String typeStr = levels[level];
        System.out.println("==== [" + typeStr + "] [" + DATE_FORMAT.format(new Date()) + "] " +
                "[" + module + "] " + msg);
    }

    public static void debug(String msg, String module) {
        Log(DEBUG, msg, module);
    }

    public static void info(String msg, String module) {
        Log(INFO, msg, module);
    }

    public static void warn(String msg, String module) {
        Log(WARN, msg, module);
    }

    public static void error(String msg, String module) {
        Log(ERROR, msg, module);
    }

    public static void critical(String msg, String module) {
        Log(CRITICAL, msg, module);
    }
}
