package com.intel.distml.util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by taotao on 15-1-14.
 *
 * Simple log system.
 */
public class Logger {
    private enum Type {
        DEBUG,
        INFO,
        WARN,
        ERROR,
        CRITICAL
    }

    private static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss.SSS");

    public static void Log(Type logType, String msg, String module) {
        String typeStr = logType.toString();
        System.out.println("==== [" + typeStr + "] [" + DATE_FORMAT.format(new Date()) + "] " +
                "[" + module + "] " + msg);
    }

    public static void debug(String msg, String module) {
        Log(Type.DEBUG, msg, module);
    }

    public static void info(String msg, String module) {
        Log(Type.INFO, msg, module);
    }

    public static void error(String msg, String module) {
        Log(Type.ERROR, msg, module);
    }

    public static void critical(String msg, String module) {
        Log(Type.CRITICAL, msg, module);
    }
}
