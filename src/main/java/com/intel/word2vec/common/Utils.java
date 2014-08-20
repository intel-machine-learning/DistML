package com.intel.word2vec.common;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Date;
import java.util.Enumeration;

/**
 * Created by spark on 7/2/14.
 */
public class Utils {
    public static String getLocalIP() throws SocketException {
        String ipAddr = "--";

        Enumeration allNetInterfaces = NetworkInterface.getNetworkInterfaces();
        InetAddress ip = null;
        while (allNetInterfaces.hasMoreElements())
        {
            NetworkInterface netInterface = (NetworkInterface) allNetInterfaces.nextElement();
            System.out.println(netInterface.getName());
            Enumeration addresses = netInterface.getInetAddresses();
            while (addresses.hasMoreElements())
            {
                ip = (InetAddress) addresses.nextElement();
                if (ip != null && ip instanceof Inet4Address)
                {
                    if (ip.getHostAddress().startsWith("192")) {
                        ipAddr = ip.getHostAddress();
                        System.out.println("IP = " + ipAddr);
                    }
                }
            }
        }

        return ipAddr;
    }

    public static void debug(String str) {
        System.out.println("[" + (new Date()) + "]" + str);
    }
}
