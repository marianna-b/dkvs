package ru.ifmo;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by mariashka on 5/17/16.
 */
class VRConfiguration {
    String[] address;
    InetSocketAddress[] inetAddresses;
    int n;
    int[] port;
    int timeout;
    VRConfiguration(String fileName) throws IOException {
        FileInputStream stream = new FileInputStream(new File(fileName));
        InputStreamReader isr = new InputStreamReader(stream, Charset.forName("UTF-8"));
        BufferedReader br = new BufferedReader(isr);
        String line;
        List<String> lines = new ArrayList<String>();
        while ((line = br.readLine()) != null) {
            lines.add(line);
        }

        n = lines.size() - 1;
        String[] last = lines.get(n).split("=");
        if (last[0].equals("timeout") && last.length == 2) {
            timeout = Integer.parseInt(last[1]);
        } else {
            throw new IOException("Invalid configuration file" + lines.get(n));
        }

        address = new String[n];
        port = new int[n];

        for (int i = 0; i < n; i++) {
            line = lines.get(i);
            String[] curr = line.split("=");
            if (curr.length == 2) {
                String[] curr1 = curr[0].split("\\.");
                String[] curr2 = curr[1].split(":");

                if (curr1.length != 2 || curr2.length != 2 || !curr1[0].equals("node")) {
                    throw new IOException("Invalid configuration file" + lines.get(i));
                } else {
                    int idx = Integer.parseInt(curr1[1]) - 1;
                    address[idx] = curr2[0];
                    port[idx] = Integer.parseInt(curr2[1]);
                    inetAddresses[idx] = new InetSocketAddress(InetAddress.getByName(address[idx]), port[idx]);
                }
            } else {
                throw new IOException("Invalid configuration file" + lines.get(i));
            }
        }
    }
}
