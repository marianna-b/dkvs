package ru.ifmo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReferenceArray;

class VRCommunications {
    private AtomicReferenceArray<Socket> nodes;
    private ConcurrentLinkedQueue<VREvent> input = new ConcurrentLinkedQueue<>();
    private ConcurrentLinkedQueue<VREvent> output = new ConcurrentLinkedQueue<>();
    private ConcurrentLinkedQueue<VREvent> delayed = new ConcurrentLinkedQueue<>();
    private int amount;
    private VRConfiguration conf;

    VRCommunications(VRConfiguration c, int idx) throws IOException {
        amount = c.n;
        conf = c;

        final ServerSocket server = new ServerSocket(conf.port[idx], 100,
                InetAddress.getByName(conf.address[idx]));

        new Thread(() -> {
            while (true) {
                try {
                    Socket socket = server.accept();
                    messageProcessing(socket);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        nodes = new AtomicReferenceArray<>(amount);
        for (int i = 0; i < amount; i++) {
            if (i != idx) {
                int finalI = i;
                new Thread(() -> {
                    while (true) {
                        Socket curr = new Socket();
                        try {
                            curr.connect(new InetSocketAddress(InetAddress.getByName(conf.address[finalI]), conf.port[finalI]), 100);
                        } catch (IOException ignored) {}
                        nodes.set(finalI, curr);
                        while (nodes.get(finalI).isConnected()) {
                            try {
                                Thread.sleep(10);
                            } catch (InterruptedException ignored) {}
                        }
                    }

                }).start();
            }
        }

        new Thread(() -> {
            while (true) {
                VREvent event = output.poll();
                if (event == null)
                    continue;
                if (event.socket == null) {
                    if (event.idx == -1)
                        continue;
                    event.socket = nodes.get(event.idx);
                }
                try {
                    event.send();
                } catch (IOException e) {
                    event.socket = null;
                    output.add(event);
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
                    input.add(new VREvent(args, null, -1));
                }
            }, 0, conf.timeout / 2);
    }

    private static final Map<String , Integer> ARGS = new HashMap<String, Integer>() {{
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

    private void messageProcessing(final Socket socket) {
       new Thread(() -> {
           try {
               BufferedReader r = new BufferedReader(new InputStreamReader(socket.getInputStream()), 100);
               String line;
               List<String> lines = new ArrayList<>();
               while ((line = r.readLine()) != null) {
                   lines.add(line);
                   int len = ARGS.get(lines.get(0));
                   if (lines.size() != len)
                       continue;
                   for (int i = 0; i < len; i++) {
                        System.out.print(lines.get(i) + " ");
                   }
                   System.out.println();
                   input.add(new VREvent(lines.subList(0, len), socket, -1));
                   lines = lines.subList(len, lines.size());
               }
           } catch (IOException e) {
               e.printStackTrace();
           }
       }).start();
   }

    void sendPrepareOK(int viewNumber, int operationNumber, int idx, int primary) {
        System.out.println("Send prepareOK from " + Integer.toString(idx) + " to primary " + Integer.toString(primary) +
        " about " + Integer.toString(operationNumber) + " view: " + Integer.toString(viewNumber));
        List<String> args = new ArrayList<>();
        args.add("prepareOK");
        args.add(Integer.toString(viewNumber));
        args.add(Integer.toString(operationNumber));
        args.add(Integer.toString(idx));
        output.add(new VREvent(args, nodes.get(primary), primary));
    }

    void sendStartView(int viewNumber, String log, int operationNumber, int commitNumber, int idx) {
        System.out.println("Send startView from " + Integer.toString(idx) + " to all about "
                + Integer.toString(operationNumber) + " view: " + Integer.toString(viewNumber));
        List<String> args = new ArrayList<>();
        args.add("startview");
        args.add(Integer.toString(viewNumber));
        args.add(log);
        args.add(Integer.toString(operationNumber));
        args.add(Integer.toString(commitNumber));

        for (int i = 0; i < amount; i++) {
            if (idx != i)
                output.add(new VREvent(args, nodes.get(i), i));
        }
    }

    void sendDoViewChange(int newViewNumber, String log, int oldViewNumber, int operationNumber, int commitNumber,
                          int idx, int primary) {
        System.out.println("Send doViewChange from " + Integer.toString(idx) + " to primary " + Integer.toString(primary)
                + " about newView: " + Integer.toString(newViewNumber) + " oldView: " + Integer.toString(oldViewNumber));

        List<String> args = new ArrayList<>();
        args.add("doviewchange");
        args.add(Integer.toString(newViewNumber));
        args.add(log);
        args.add(Integer.toString(oldViewNumber));
        args.add(Integer.toString(operationNumber));
        args.add(Integer.toString(commitNumber));
        args.add(Integer.toString(idx));

        output.add(new VREvent(args, nodes.get(primary), primary));
    }

    void sendGetState(int viewNumber, int operationNumber, int idx, int nodeNumber) {
        System.out.println("Send getView from " + Integer.toString(idx) + " to node " + Integer.toString(nodeNumber)
                + " about operations after: " + Integer.toString(operationNumber));
        List<String> args = new ArrayList<>();
        args.add(Integer.toString(viewNumber));
        args.add(Integer.toString(operationNumber));
        args.add(Integer.toString(idx));

        output.add(new VREvent(args, nodes.get(nodeNumber), nodeNumber));
    }

    VREvent waitForType(String type) {
        System.out.println("Wait for " + type + " message");
        VREvent res;
        while ((res = input.poll()) == null || !res.type.equals(type)) {
            if (res != null)
                delayed.add(res);
        }
        System.out.println("Received!");
        return res;
    }

    VREvent getEvent() {
        while (true) {
            VREvent event = delayed.poll();
            if (event == null)
                event = input.poll();
            if (event == null) {
                try {
                    Thread.sleep(5);
                } catch (InterruptedException ignored) {}
                continue;
            }
            return event;
        }
    }

    void replyToClient(int clientID, VREvent event, int viewNumber, String result) {
        System.out.println("Send reply to clientID " + Integer.toString(clientID) + " with " + result);

        List<String> args = new ArrayList<>();
        args.add("reply");
        args.add(Integer.toString(viewNumber));
        args.add(Integer.toString(clientID));
        args.add(result);

        output.add(new VREvent(args, event.socket, -1));
    }

    VREvent sendStartViewChange(int newViewNumber, int idx) {
        System.out.println("Send startViewCHange from " + Integer.toString(idx) + " to all with view: "
                + Integer.toString(newViewNumber));
        List<String> args = new ArrayList<>();
        args.add("startviewchange");
        args.add(Integer.toString(newViewNumber));
        args.add(Integer.toString(idx));

        for (int i = 0; i < nodes.length(); i++) {
            if (i != idx)
                output.add(new VREvent(args, nodes.get(i), i));
        }

        return new VREvent(args, nodes.get(idx), idx);
    }

    void sendPrepare(String operation, int clientID, int requestNumber, int viewNumber, int operationNumber,
                     int commitNumber, int idx) {
        System.out.println("Send prepare from primary " + Integer.toString(idx) + " to all with clientID: "
                + Integer.toString(clientID) + " reqNum: " + Integer.toString(requestNumber) + " operation: " + operation +
                Integer.toString(requestNumber) + " commNum: " + Integer.toString(commitNumber) +
                " view: " + Integer.toString(viewNumber) + " opNum: " + Integer.toString(operationNumber));
        List<String> args = new ArrayList<>();
        args.add("prepare");
        args.add(Integer.toString(viewNumber));
        args.add(operation);
        args.add(Integer.toString(operationNumber));
        args.add(Integer.toString(commitNumber));
        args.add(Integer.toString(clientID));
        args.add(Integer.toString(requestNumber));

        for (int i = 0; i < nodes.length(); i++) {
            if (i != idx)
                output.add(new VREvent(args, nodes.get(i), i));
        }
    }

    void sendState(int idx, int viewNumber, int operationNumber, int commitNumber, String log) {
        System.out.println("Sending current state to " + Integer.toString(idx) + " with" +
        " view: " + Integer.toString(viewNumber) + " opNum: " + Integer.toString(operationNumber) +
        " comNum: " + Integer.toString(commitNumber));
        List<String> args = new ArrayList<>();
        args.add("newstate");
        args.add(Integer.toString(viewNumber));
        args.add(log);
        args.add(Integer.toString(operationNumber));
        args.add(Integer.toString(commitNumber));

        output.add(new VREvent(args, nodes.get(idx), idx));
    }

    void sendCommit(int viewNumber, int commitNumber, int idx) {
        System.out.println("Send commit from " + Integer.toString(idx) + " with commNum " + Integer.toString(commitNumber));
        List<String> args = new ArrayList<>();
        args.add("commit");
        args.add(Integer.toString(viewNumber));
        args.add(Integer.toString(commitNumber));

        for (int i = 0; i < nodes.length(); i++) {
            if (i != idx)
                output.add(new VREvent(args, nodes.get(i), i));
        }
    }
}
