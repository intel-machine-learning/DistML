package com.intel.distml.util;

import java.io.DataInputStream;
import java.io.IOException;
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

    public static String getLocalIP(String networkPrefix) throws SocketException {
        String ipAddr = "--";

        Enumeration allNetInterfaces = NetworkInterface.getNetworkInterfaces();
        InetAddress ip = null;
        while (allNetInterfaces.hasMoreElements())
        {
            NetworkInterface netInterface = (NetworkInterface) allNetInterfaces.nextElement();
            Enumeration addresses = netInterface.getInetAddresses();
            while (addresses.hasMoreElements())
            {
                ip = (InetAddress) addresses.nextElement();
                if (ip != null && ip instanceof Inet4Address)
                {
                    if (networkPrefix != null) {
                        if (ip.getHostAddress().startsWith(networkPrefix)) {
                            ipAddr = ip.getHostAddress();
                            System.out.println("Server IP = " + ipAddr);
                        }
                    }
                    else {
                        if (!ip.getHostAddress().startsWith("127")) {
                            ipAddr = ip.getHostAddress();
                            System.out.println("Server IP = " + ipAddr);
                        }
                    }
                }
            }
        }

        return ipAddr;
    }

    public static void waitUntil(DataInputStream is, int size) throws IOException {
        while(is.available() < size) { try { Thread.sleep(1); } catch (Exception e){}}
    }
}
