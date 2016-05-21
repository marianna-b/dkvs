package ru.ifmo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by mariashka on 5/21/16.
 */
public class VRCommunications {
    private Socket[] nodes;
    private ConcurrentLinkedQueue<VREvent> input = new ConcurrentLinkedQueue<>();
    private ConcurrentLinkedQueue<VREvent> output = new ConcurrentLinkedQueue<>();
    private ConcurrentLinkedQueue<VREvent> delayed = new ConcurrentLinkedQueue<>();
    private VRConfiguration configuration;
    private int amount;

    VRCommunications(VRConfiguration conf, int idx) throws IOException {
        amount = conf.n;
        configuration = conf;

        // TODO fix sockets
        nodes = new Socket[amount];
        for (int i = 0; i < amount; i++) {
            nodes[i] = new Socket(InetAddress.getByName(configuration.address[idx]), configuration.port[idx]);

            try {
                nodes[i].connect(new InetSocketAddress(InetAddress.getByName(configuration.address[i]),
                        configuration.port[i]), 200);
                messageProcessing(nodes[i]);
            } catch (IOException ignored) {
                nodes[i] = null;
            }
        }


        final ServerSocket server = new ServerSocket(configuration.port[idx], 100,
                InetAddress.getByName(configuration.address[idx]));

        new Thread(() -> {
            while (true) {
                try {
                    // TODO fix socket
                    Socket socket = server.accept();
                    messageProcessing(socket);
                    for (int i = 0; i < configuration.n; i++) {
                        if (nodes[i] == null
                                && socket.getInetAddress().equals(configuration.inetAddresses[i].getAddress())
                                && socket.getPort() == configuration.inetAddresses[i].getPort()) {
                            nodes[i] = socket;
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
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

        Timer t = new Timer();
        t.scheduleAtFixedRate(
            new TimerTask()
            {
                public void run()
                {
                    List<String> args = new ArrayList<>();
                    args.add("ping");
                    input.add(new VREvent(args, null));
                }
            }, 0, configuration.timeout / 2);
    }

    // TODO check numbers
    private static final Map<String , Integer> ARGS = new HashMap<>() {{
        put("request", 4);
        put("prepare", 7);
        put("prepareOK", 4);
        put("reply", 4);
        put("commit", 3);
        put("getstate", 4);
        put("newstate", 5);
        put("startviewchange", 3);
        put("doviewchange", 7);
        put("startview", 5);
        put("ping", 1);
    }};

    // TODO fix sockets
    void messageProcessing(final Socket socket) {
       new Thread(() -> {
           List<String> strings = new ArrayList<>();
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


    void sendPrepareOK(int viewNumber, int operationNumber, int idx, int primary) {
        List<String> args = new ArrayList<>();
        args.add("prepareOK");
        args.add(Integer.toString(viewNumber));
        args.add(Integer.toString(operationNumber));
        args.add(Integer.toString(idx));
        output.add(new VREvent(args, nodes[primary])); // TODO socket of primary
    }

    void sendStartView(int viewNumber, String log, int operationNumber, int commitNumber) {
        List<String> args = new ArrayList<>();
        args.add("startview");
        args.add(Integer.toString(viewNumber));
        args.add(log);
        args.add(Integer.toString(operationNumber));
        args.add(Integer.toString(commitNumber));

        for (Socket node : nodes) {
            // TODO dont send to primary and check
            output.add(new VREvent(args, node));
        }
    }

    void sendDoViewChange(int newViewNumber, String log, int oldViewNumber, int operationNumber, int commitNumber,
                          int idx, int primary) {
        List<String> args = new ArrayList<>();
        args.add("doviewchange");
        args.add(Integer.toString(newViewNumber));
        args.add(log);
        args.add(Integer.toString(oldViewNumber));
        args.add(Integer.toString(operationNumber));
        args.add(Integer.toString(commitNumber));
        args.add(Integer.toString(idx));

        output.add(new VREvent(args, nodes[primary]));
    }

    void sendGetState(int viewNumber, int operationNumber, int idx, int nodeNumber) {
        List<String> args = new ArrayList<>();
        args.add(Integer.toString(viewNumber));
        args.add(Integer.toString(operationNumber));
        args.add(Integer.toString(idx));

        output.add(new VREvent(args, nodes[nodeNumber]));
    }

    VREvent waitForType(String type) {
        VREvent res;
        while ((res = input.poll()) == null || !res.type.equals(type)) {
            if (res != null)
                delayed.add(res);
        }
        return res;
    }

    // TODO blocking not return null
    VREvent getEvent() {
        while (true) {
            VREvent event = delayed.poll();
            if (event == null)
                event = input.poll();
            if (event == null)
                continue;
            return event;
        }
    }


    void replyToClient(int clientID, VREvent event, int viewNumber, String result) {
        List<String> args = new ArrayList<>();
        args.add("reply");
        args.add(Integer.toString(viewNumber));
        args.add(Integer.toString(clientID));
        args.add(result);

        output.add(new VREvent(args, event.socket)); // TODO socket from event
    }

    VREvent sendStartViewChange(int newViewNumber, int idx) {
        List<String> args = new ArrayList<>();
        args.add("startviewchange");
        args.add(Integer.toString(newViewNumber));
        args.add(Integer.toString(idx));

        for (int i = 0; i < nodes.length; i++) {
            if (i != idx)
                output.add(new VREvent(args, nodes[i])); // TODO socket of node
        }

        return new VREvent(args, nodes[idx]); // TODO socket from node
    }


    void sendPrepare(String operation, int clientID, int requestNumber, int viewNumber, int operationNumber,
                     int commitNumber, int idx) {
        List<String> args = new ArrayList<>();
        args.add("prepare");
        args.add(Integer.toString(viewNumber));
        args.add(operation);
        args.add(Integer.toString(operationNumber));
        args.add(Integer.toString(commitNumber));
        args.add(Integer.toString(clientID));
        args.add(Integer.toString(requestNumber));

        for (int i = 0; i < nodes.length; i++) {
            if (i != idx)
                output.add(new VREvent(args, nodes[i])); // TODO socket of node
        }
    }


    void sendState(int idx, int viewNumber, int operationNumber, int commitNumber, String log) {
        List<String> args = new ArrayList<>();
        args.add("newstate");
        args.add(Integer.toString(viewNumber));
        args.add(log);
        args.add(Integer.toString(operationNumber));
        args.add(Integer.toString(commitNumber));

        output.add(new VREvent(args, nodes[idx])); // TODO socket of node
    }

    void sendCommit(int viewNumber, int commitNumber, int idx) {
        List<String> args = new ArrayList<>();
        args.add("commit");
        args.add(Integer.toString(viewNumber));
        args.add(Integer.toString(commitNumber));

        for (int i = 0; i < nodes.length; i++) {
            if (i != idx)
                output.add(new VREvent(args, nodes[i])); // TODO socket of node
        }
    }
}
