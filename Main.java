package ru.ifmo;

import java.io.IOException;

public class Main {

    public static void main(String[] args) {
        try {
            VRConfiguration configuration = new VRConfiguration("dkvs.properties");
            VRServer[] servers = new VRServer[configuration.n];
            if (args.length < 1) {
                for (int i = 0; i < configuration.n; i++) {
                    servers[i] = new VRServer(configuration, i);
                }
            } else {
                int idx = Integer.parseInt(args[0]) - 1;
                servers[idx] = new VRServer(configuration, idx);
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
}
