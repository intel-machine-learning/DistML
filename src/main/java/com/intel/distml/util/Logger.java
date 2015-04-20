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
        ERROR
    }

    public enum Role {
        APP,
        DATABUS,
        WORKER,
        WORKER_LEAD,
        PARAMETER_SERVER,
        MONITOR,
        SYSTEM
    }

    private static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss.SSS");
    private static void Log(Type logType, String msg, Role role, int index) {
        String typeStr = logType.toString();
        String roleStr = role.toString();

        if (logType == Type.ERROR)
            System.err.println("==== [" + typeStr + "] [" + DATE_FORMAT.format(new Date()) + "] " +
                    "[" + roleStr + "-" + index + "] " + msg);
        else
            System.out.println("==== [" + typeStr + "] [" + DATE_FORMAT.format(new Date()) + "] " +
                "[" + roleStr + "-" + index + "] " + msg);
    }

    public static void DebugLog(String msg, Role role, int index) {
        Log(Type.DEBUG, msg, role, index);
    }

    public static void InfoLog(String msg, Role role, int index) {
        Log(Type.INFO, msg, role, index);
    }

    public static void WarnLog(String msg, Role role, int index) {
        Log(Type.WARN, msg, role, index);
    }

    public static void ErrorLog(String msg, Role role, int index) {
        Log(Type.ERROR, msg, role, index);
    }
}
