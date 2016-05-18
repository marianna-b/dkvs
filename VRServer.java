package ru.ifmo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by mariashka on 5/17/16.
 */
public class VRServer {
    private int idx;
    private VRConfiguration configuration;
    private Socket[] nodes;
    private ConcurrentLinkedQueue<VREvent> input;
    private ConcurrentLinkedQueue<VREvent> output;

    private static final Map<String , Integer> ARGS = new HashMap<String , Integer>() {{
        put("request", 3);
        put("prepare", 4);
        put("prepareOK", 3);
        put("reply", 3);
        put("commit", 2);
        put("getstate", 3);
        put("newstate", 4);
        put("startviewchange", 2);
        put("doviewchange", 6);
        put("startview", 4);
    }};


    static final Set<String> REQ = new HashSet<String>() {{
        add("get");
        add("set");
        add("delete");
        add("ping");
    }};


   private void messageProcessing(final Socket socket) {
       new Thread(() -> {
           List<String> strings = new ArrayList<String>();
           try {
               BufferedReader r = new BufferedReader(new InputStreamReader(socket.getInputStream()), 100);
               String sb = "";
               int ch;
               while ((ch = r.read()) != -1) {
                   char curr = (char)ch;
                    if (curr != ' ') {
                        sb += (char) ch;
                        continue;
                    }
                    strings.add(sb);
                    sb = "";
                   if (ARGS.containsKey(strings.get(0))) {
                        strings.remove(0);
                   } else {
                       int amount = ARGS.get(strings.get(0));
                       if (strings.size() >= amount) {
                           input.add(new VREvent(strings.subList(0, amount), socket));
                           strings = strings.subList(amount, strings.size());
                       }
                   }
               }
           } catch (IOException e) {
               e.printStackTrace();
           }
       }).start();
   }

    private void request(VREvent event) {
    }
    private void prepare(VREvent event) {
    }
    private void prepareOK(VREvent event) {
    }
    private void reply(VREvent event) {
    }
    private void startView(VREvent event) {
    }
    private void startViewChange(VREvent event) {
    }
    private void doViewChange(VREvent event) {
    }
    private void getState(VREvent event) {
    }
    private void newState(VREvent event) {
    }
    private void commit(VREvent event) {
    }


    private void eventProcess(VREvent event) {
        switch (event.type) {
            case "request":
                request(event);
                break;
            case "prepare":
                prepare(event);
                break;
            case "prepareOK":
                prepareOK(event);
                break;
            case "startview":
                startView(event);
                break;
            case "doviewchange":
                doViewChange(event);
                break;
            case "startviewchange":
                startViewChange(event);
                break;
            case "newstate":
                newState(event);
                break;
            case "getstate":
                getState(event);
                break;
            case "commit":
                commit(event);
                break;
            case "reply":
                reply(event);
                break;
            default:
        }
    }

    private void main() {
        new Thread(() -> {
           while (true) {
               VREvent event = input.poll();
               if (event != null)
                   eventProcess(event);
           }
        }).start();
    }

    VRServer(VRConfiguration conf, int x) throws IOException {
        configuration = conf;
        idx = x;
        final ServerSocket server = new ServerSocket(configuration.port[idx], 100, InetAddress.getByName(configuration.address[idx]));
        nodes = new Socket[configuration.n];

        for (int i = 0; i < configuration.n; i++) {
            nodes[i] = new Socket(InetAddress.getByName(configuration.address[idx]), configuration.port[idx]);

            try {
                nodes[i].connect(new InetSocketAddress(InetAddress.getByName(configuration.address[i]), configuration.port[i]), 200);
                messageProcessing(nodes[i]);
            } catch (IOException ignored) {
                nodes[i] = null;
            }
        }

        new Thread(() -> {
            try {
                Socket socket = server.accept();
                messageProcessing(socket);
                for (int i = 0; i < configuration.n; i++) {
                    if (nodes[i] == null && socket.getInetAddress().equals(configuration.inetAddresses[i].getAddress())
                            && socket.getPort() == configuration.inetAddresses[i].getPort()) {
                        nodes[i] = socket;
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
        new Thread(() -> {
            while (true) {
                try {
                    VREvent event = output.poll();
                    if (event != null)
                        event.send();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();
        main();
    }
}
