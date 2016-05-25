package ru.ifmo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReferenceArray;

class VRCommunications {
    private AtomicReferenceArray<Socket> nodes;
    private ConcurrentLinkedQueue<VREvent> input = new ConcurrentLinkedQueue<>();
    private ConcurrentLinkedQueue<VREvent> output = new ConcurrentLinkedQueue<>();
    private ArrayList <ConcurrentLinkedQueue<VREvent> > socketOut;
    private ConcurrentHashMap<SocketAddress, List<String> > lists = new ConcurrentHashMap<>();
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
                    SocketAddress addr = socket.getRemoteSocketAddress();
                    if (!lists.containsKey(addr))
                        lists.put(addr, new ArrayList<>());
                    messageProcessing(socket);
                } catch (IOException ignored) {}
            }
        }).start();

        nodes = new AtomicReferenceArray<>(amount);
        socketOut = new ArrayList<>();

        for (int i = 0; i < amount; i++) {
            socketOut.add(new ConcurrentLinkedQueue<>());
            nodes.set(i, new Socket());
            if (i != idx) {
                int finalI1 = i;
                tryReconnect(finalI1);

                new Thread(() -> {
                    ConcurrentLinkedQueue<VREvent> q = socketOut.get(finalI1);
                    VREvent event = q.poll();
                    while (true) {
                        if (event == null) {
                            try {
                                Thread.sleep(5);
                            } catch (InterruptedException ignored) {}
                            tryReconnect(finalI1);
                            event = q.poll();
                            continue;
                        }
                        try {
                            event.send();
                            event = q.poll();
                        } catch (IOException ignored) {
                            //noinspection EmptyCatchBlock
                            tryReconnect(finalI1);
                            event.socket = nodes.get(finalI1);
                            //noinspection EmptyCatchBlock
                            try {
                                event.send();
                            } catch (IOException ign2) {}
                            event = q.poll();
                        }
                    }
                }).start();

            }
        }

        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ignored) {}
                VREvent event = output.poll();
                if (event == null) {
                    continue;
                }
                if (event.idx != -1) {
                    socketOut.get(event.idx).add(event);
                    continue;
                }
                try {
                    event.send();
                } catch (IOException ignored) {}
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
                    input.add(new VREvent(new ArrayList<>(args), null, -1));
                }
            }, 50, conf.timeout / 3);
    }

    private static final ConcurrentHashMap<String , Integer> ARGS = new ConcurrentHashMap<String, Integer>() {{
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
               List<String> lines = lists.get(socket.getRemoteSocketAddress());
               while ((line = r.readLine()) != null) {
                   lines.add(line);
                   lists.put(socket.getRemoteSocketAddress(), lines);

                   while (lines.size() > 0 && !ARGS.containsKey(lines.get(0))) {
                       lines.remove(0);
                   }
                    if (lines.size() == 0)
                        continue;

                   int len = ARGS.get(lines.get(0));
                   if (lines.size() < len)
                       continue;

                   input.add(new VREvent(new ArrayList<>(lines.subList(0, len)), socket, -1));
                   lines = lines.subList(len, lines.size());
               }
           } catch (IOException ignored) {
               try {
                   socket.close();
               } catch (IOException ignored2) {}
           }
       }).start();
   }


    private void tryReconnect(int i) {
        try {
            Thread.sleep(130);
        } catch (InterruptedException ignored) {
        }
        try {
            Socket tmp = nodes.getAndSet(i, new Socket());
            if (tmp != null) {
                tmp.close();
            }
            nodes.get(i).connect(new InetSocketAddress(InetAddress.getByName(conf.address[i]),
                    conf.port[i]), 50);
        } catch (IOException ignored) {
        }
    }

    void sendPrepareOK(int viewNumber, int operationNumber, int idx, int primary) {
        System.out.println("Send prepareOK from " + Integer.toString(idx) + " to primary " + Integer.toString(primary) +
        " about " + Integer.toString(operationNumber) + " view: " + Integer.toString(viewNumber));
        List<String> args = new ArrayList<>();
        args.add("prepareOK");
        args.add(Integer.toString(viewNumber));
        args.add(Integer.toString(operationNumber));
        args.add(Integer.toString(idx));
        output.add(new VREvent(new ArrayList<>(args), nodes.get(primary), primary));
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
                output.add(new VREvent(new ArrayList<>(args), nodes.get(i), i));
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

        output.add(new VREvent(new ArrayList<>(args), nodes.get(primary), primary));
    }

    void sendGetState(int viewNumber, int operationNumber, int idx) {
        System.out.println("Send getView from " + Integer.toString(idx) + " to nodes "
                + " about operations after: " + Integer.toString(operationNumber));
        List<String> args = new ArrayList<>();
        args.add("getstate");
        args.add(Integer.toString(viewNumber));
        args.add(Integer.toString(operationNumber));
        args.add(Integer.toString(idx));

        for (int i = 0; i < amount; i++) {
            if (idx != i)
                output.add(new VREvent(new ArrayList<>(args), nodes.get(i), i));
        }
    }
    /*
    ru.ifmo.VREvent waitForType(String type) {
        System.out.println("Wait for " + type + " message");
        ru.ifmo.VREvent res;
        while ((res = input.poll()) == null || !res.type.equals(type)) {
            if (res != null)
                delayed.add(res);
        }
        System.out.println("Received!");
        return res;
    }
    */
    VREvent getEvent() {
        while (true) {
            VREvent event = input.poll();
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

        output.add(new VREvent(new ArrayList<>(args), event.socket, -1));
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

        return new VREvent(new ArrayList<>(args), nodes.get(idx), idx);
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
                output.add(new VREvent(new ArrayList<>(args), nodes.get(i), i));
        }
    }
}
