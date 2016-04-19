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

    public static String[] getNetworkAddress(String networkPrefix) throws SocketException {
        String[] addr = new String[2];

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
                            addr[0] = ip.getHostAddress();
                            addr[1] = ip.getHostName();
                            System.out.println("Server address = " + addr[0] + ", " + addr[1]);
                        }
                    }
                    else {
                        if (!ip.getHostAddress().startsWith("127")) {
                            addr[0] = ip.getHostAddress();
                            addr[1] = ip.getHostName();
                            System.out.println("Server address = " + addr[0] + ", " + addr[1]);
                        }
                    }
                }
            }
        }

        return addr;
    }

    public static void waitUntil(DataInputStream is, int size) throws IOException {
        while(is.available() < size) { try { Thread.sleep(1); } catch (Exception e){}}
    }
}
