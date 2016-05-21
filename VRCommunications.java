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

class VRCommunications {
    private Socket[] nodes;
    private ConcurrentLinkedQueue<VREvent> input = new ConcurrentLinkedQueue<>();
    private ConcurrentLinkedQueue<VREvent> output = new ConcurrentLinkedQueue<>();
    private ConcurrentLinkedQueue<VREvent> delayed = new ConcurrentLinkedQueue<>();
    private int amount;
    private VRConfiguration conf;

    VRCommunications(VRConfiguration c, int idx) throws IOException {
        amount = conf.n;
        conf = c;

        nodes = new Socket[amount];
        for (int i = 0; i < amount; i++) {
            nodes[i] = new Socket();
            nodes[i].connect(new InetSocketAddress(InetAddress.getByName(conf.address[i]),
                        conf.port[i]));
        }

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

        new Thread(() -> {
            while (true) {
                VREvent event = output.poll();
                try {
                    if (event != null)
                        event.send();
                } catch (IOException e) {
                    if (event.idx == -1)
                        continue;
                    nodes[event.idx] = new Socket();
                    try {
                        nodes[idx].connect(new InetSocketAddress(InetAddress.getByName(conf.address[idx]), conf.port[idx]));
                        event.socket = nodes[idx];
                        event.send();
                    } catch (IOException ignored) {}
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
                   input.add(new VREvent(lines.subList(0, len), socket, -1));
                   lines = lines.subList(len, lines.size() - len);
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
        output.add(new VREvent(args, nodes[primary], primary));
    }

    void sendStartView(int viewNumber, String log, int operationNumber, int commitNumber, int idx) {
        List<String> args = new ArrayList<>();
        args.add("startview");
        args.add(Integer.toString(viewNumber));
        args.add(log);
        args.add(Integer.toString(operationNumber));
        args.add(Integer.toString(commitNumber));

        for (int i = 0; i < amount; i++) {
            if (idx != i)
                output.add(new VREvent(args, nodes[i], i));
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

        output.add(new VREvent(args, nodes[primary], primary));
    }

    void sendGetState(int viewNumber, int operationNumber, int idx, int nodeNumber) {
        List<String> args = new ArrayList<>();
        args.add(Integer.toString(viewNumber));
        args.add(Integer.toString(operationNumber));
        args.add(Integer.toString(idx));

        output.add(new VREvent(args, nodes[nodeNumber], nodeNumber));
    }

    VREvent waitForType(String type) {
        VREvent res;
        while ((res = input.poll()) == null || !res.type.equals(type)) {
            if (res != null)
                delayed.add(res);
        }
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
        List<String> args = new ArrayList<>();
        args.add("reply");
        args.add(Integer.toString(viewNumber));
        args.add(Integer.toString(clientID));
        args.add(result);

        output.add(new VREvent(args, event.socket, -1));
    }

    VREvent sendStartViewChange(int newViewNumber, int idx) {
        List<String> args = new ArrayList<>();
        args.add("startviewchange");
        args.add(Integer.toString(newViewNumber));
        args.add(Integer.toString(idx));

        for (int i = 0; i < nodes.length; i++) {
            if (i != idx)
                output.add(new VREvent(args, nodes[i], i));
        }

        return new VREvent(args, nodes[idx], idx);
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
                output.add(new VREvent(args, nodes[i], i));
        }
    }

    void sendState(int idx, int viewNumber, int operationNumber, int commitNumber, String log) {
        List<String> args = new ArrayList<>();
        args.add("newstate");
        args.add(Integer.toString(viewNumber));
        args.add(log);
        args.add(Integer.toString(operationNumber));
        args.add(Integer.toString(commitNumber));

        output.add(new VREvent(args, nodes[idx], idx));
    }

    void sendCommit(int viewNumber, int commitNumber, int idx) {
        List<String> args = new ArrayList<>();
        args.add("commit");
        args.add(Integer.toString(viewNumber));
        args.add(Integer.toString(commitNumber));

        for (int i = 0; i < nodes.length; i++) {
            if (i != idx)
                output.add(new VREvent(args, nodes[i], i));
        }
    }
}
