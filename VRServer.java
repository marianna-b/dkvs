package ru.ifmo;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

class VRServer {
    private VRStatus S;

    private void sendPrepare(String operation, int clientID, int requestNumber) {
        List<String> args = new ArrayList<>();
        args.add("prepare");
        args.add(Integer.toString(S.viewNumber));
        args.add(operation);
        args.add(Integer.toString(S.operationNumber));
        args.add(Integer.toString(S.commitNumber));
        args.add(Integer.toString(clientID));
        args.add(Integer.toString(requestNumber));

        for (int i = 0; i < S.nodes.length; i++) {
            S.output.add(new VREvent(args, S.nodes[i]));
        }
    }
    private void sendPrepareOK(Socket socket, int operationNumber) {
        List<String> args = new ArrayList<>();
        args.add("prepareOK");
        args.add(Integer.toString(S.viewNumber));
        args.add(Integer.toString(operationNumber));
        args.add(Integer.toString(S.idx));
        S.output.add(new VREvent(args, socket));
    }

    private void sendCommit() {
        List<String> args = new ArrayList<>();
        args.add("commit");
        args.add(Integer.toString(S.viewNumber));
        args.add(Integer.toString(S.commitNumber));

        for (int i = 0; i < S.nodes.length; i++) {
            S.output.add(new VREvent(args, S.nodes[i]));
        }
    }
    private void sendState(int operation, int idx) {
        List<String> args = new ArrayList<>();
        args.add("newstate");
        args.add(Integer.toString(S.viewNumber));
        args.add(S.getLogAfter(operation));
        args.add(Integer.toString(S.operationNumber));
        args.add(Integer.toString(S.commitNumber));

        S.output.add(new VREvent(args, S.nodes[idx]));
    }

    private void request(VREvent event) {
        String operation = event.args.get(0);
        int clientID = Integer.parseInt(event.args.get(1));
        int requestNumber = Integer.parseInt(event.args.get(2));

        if (!S.clientTable.containsKey(clientID))
            S.clientTable.put(clientID, 0);
        if (S.clientTable.get(clientID) > requestNumber)
            return;
        if (S.clientTable.get(clientID) == requestNumber) {

            List<String> args = new ArrayList<>();
            args.add("reply");
            args.add(Integer.toString(S.viewNumber));
            args.add(Integer.toString(clientID));
            args.add(S.clientResult.get(clientID));

            S.output.add(new VREvent(args, event.socket));

            return;
        }
        S.clients.put(S.operationNumber, event);
        S.clientTable.put(clientID, requestNumber);
        S.clientResult.remove(clientID);
        S.operationNumber++;
        S.setupCounter(S.operationNumber);
        sendPrepare(operation, clientID, requestNumber);
    }

    private void prepare(VREvent event) {
        int v = Integer.parseInt(event.args.get(0));
        String operation = event.args.get(1);
        int n = Integer.parseInt(event.args.get(2));
        int k = Integer.parseInt(event.args.get(3));
        int clientID = Integer.parseInt(event.args.get(3));
        int requestNumber = Integer.parseInt(event.args.get(4));

        if (S.isBehind(v, n - 1))
            return;
        if (!S.checkUpToDate(v, n - 1))
            S.updateTO(v, n - 1);
        if (S.isPrimary())
            return;
        if (S.commitNumber < k)
            S.updateWith(null, v, S.operationNumber, k);

        S.accessPrimary = true;
        S.operationNumber++;
        S.clientTable.put(clientID, requestNumber);
        S.clientResult.remove(clientID);
        S.addToLog(clientID, requestNumber, operation);
        sendPrepareOK(event.socket, S.operationNumber);
    }

    private void prepareOK(VREvent event) {
        int v = Integer.parseInt(event.args.get(0));
        int n = Integer.parseInt(event.args.get(1));
        int i = Integer.parseInt(event.args.get(2));

        if (S.isBehind(v, n))
            return;

        if (!S.checkUpToDate(v, n))
            S.updateTO(v, n);
        if (S.isPrimary())
            S.addCounter(n, i);
    }


    private void getState(VREvent event) {
        int v = Integer.parseInt(event.args.get(0));
        int n = Integer.parseInt(event.args.get(1));
        int i = Integer.parseInt(event.args.get(2));

        if (S.viewNumber != v)
            return;
        if (S.isNormal())
            sendState(n, i);
    }

    private void newState(VREvent event) {
        int v = Integer.parseInt(event.args.get(0));
        String l = event.args.get(1);
        int n = Integer.parseInt(event.args.get(2));
        int k = Integer.parseInt(event.args.get(3));

        if (S.isBehind(v, n))
            return;

        if (S.isRecovering())
            S.updateWith(l, v, n, k);
    }

    private void commit(VREvent event) {
        int v = Integer.parseInt(event.args.get(0));
        int k = Integer.parseInt(event.args.get(1));

        if (S.isBehind(v, k))
            return;

        if (!S.checkUpToDate(v, k))
            S.updateTO(v, k);

        S.accessPrimary = true;
        S.commitToOperation(k);
    }

    private void ping(VREvent event) {
        if (S.isPrimary()) {
            sendCommit();
            return;
        }
        if (!S.accessPrimary())
            S.startViewChange(event);
    }

    private void startView(VREvent event) {
        int v = Integer.getInteger(event.args.get(0));
        String l = event.args.get(1);
        int n = Integer.getInteger(event.args.get(2));
        int k = Integer.getInteger(event.args.get(3));

        S.replaceState(v, l, n, k);
        for (int i = S.commitNumber + 1; i <= S.operationNumber; i++) {
            sendPrepareOK(S.nodes[S.getPrimary()], i);
        }
    }

    private void startViewChange(VREvent event) {
        S.startViewChange(event);
    }

    private void doViewChange(VREvent event) {
        S.startViewChange(event);
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
            case "ping":
                ping(event);
                break;
            default:
        }
    }

    private void main() {
        while (true) {
            VREvent event = S.delayed.poll();
            if (event == null)
                event = S.input.poll();
            if (event == null)
                continue;
            if (S.isPrimary())
                S.checkAndCommit();
            eventProcess(event);
        }
    }

    VRServer(VRConfiguration conf, int x) throws IOException {
        S = new VRStatus(conf, x);

        final ServerSocket server = new ServerSocket(S.configuration.port[S.idx], 100,
                InetAddress.getByName(S.configuration.address[S.idx]));

        new Thread(() -> {
            try {
                Socket socket = server.accept();
                S.messageProcessing(socket);
                for (int i = 0; i < S.configuration.n; i++) {
                    if (S.nodes[i] == null
                            && socket.getInetAddress().equals(S.configuration.inetAddresses[i].getAddress())
                            && socket.getPort() == S.configuration.inetAddresses[i].getPort()) {
                        S.nodes[i] = socket;
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();

        new Thread(() -> {
            while (true) {
                try {
                    VREvent event = S.output.poll();
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
                    S.input.add(new VREvent(args, null));
                }
            }, 0, S.configuration.timeout / 2);
        main();
    }
}
