package ru.ifmo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

class VRStatus {


    private enum State {
        NORMAL,
        VIEWCHANGE,
        RECOVERING
    }

    int idx;
    VRConfiguration configuration;
    Socket[] nodes;
    ConcurrentLinkedQueue<VREvent> input;
    ConcurrentLinkedQueue<VREvent> output;
    Queue<VREvent> delayed;
    private VRLog log;

    Map<Integer, Integer> clientTable = new HashMap<>();
    Map<Integer, String> clientResult = new HashMap<>();
    Map<Integer, VREvent> clients;
    private Map<Integer, Integer> counter = new HashMap<>();
    private Map<Integer, List<Integer> > prepared = new HashMap<>();

    int operationNumber = 0;
    int commitNumber = 0;
    int viewNumber = 0;
    private State state = State.NORMAL;
    boolean accessPrimary;

    int getPrimary() {
        return viewNumber % configuration.n;
    }


    boolean isRecovering() {
        return (state == State.RECOVERING);
    }

    boolean isPrimary() {
        return (viewNumber % configuration.n == idx);
    }

    boolean accessPrimary() {
        if (!accessPrimary)
            return false;
        else {
            accessPrimary = false;
            return true;
        }
    }

    void replaceState(int v, String l, int n, int k) {
        state = State.NORMAL;
        log.replace(l, k);
        operationNumber = n;
        commitNumber = k;
        accessPrimary = true;
        clientResult = new HashMap<>();
        clientTable = new HashMap<>();
    }

    void updateTO(int needView, int needOperation) {
        state = State.RECOVERING;
        // TODO
    }

    void startViewChange(VREvent event) {
        state = State.VIEWCHANGE;
        // TODO
    }

    boolean checkUpToDate(int needView, int needOperation) {
        return needView <= viewNumber && needOperation <= operationNumber;
    }

    void updateWith(String newLog, int newView, int newOpNumber, int newCommitNumber) {
        log.appendLog(newLog);
        viewNumber = newView;
        operationNumber = newOpNumber;
        commitToOperation(newCommitNumber);
        state = State.NORMAL;
    }

    void commitToOperation(int newCommitNumber) {
        for (int i = commitNumber + 1; i <= newCommitNumber ; i++) {
            log.invokeOperation(i);
        }
        commitNumber = newCommitNumber;
    }

    void checkAndCommit() {
        if (operationNumber == commitNumber)
            return;
        if (counter.get(commitNumber + 1) < configuration.n / 2)
            return;
        counter.remove(commitNumber + 1);
        prepared.remove(commitNumber + 1);
        String res = log.invokeOperation(commitNumber + 1);
        VREvent event = clients.get(commitNumber + 1);
        replyToClient(Integer.parseInt(event.args.get(3)), res, event);
    }

    void setupCounter(int operationNumber) {
        counter.put(operationNumber, 0);
        prepared.put(operationNumber, new ArrayList<>(configuration.n));
        prepared.get(operationNumber).set(idx, 1);
    }

    void addCounter(int operationNumber, int backupIdx) {
        if (!prepared.containsKey(operationNumber))
            return;
        if (prepared.get(operationNumber).get(backupIdx) == 1)
            return;
        int count = counter.get(operationNumber) + 1;
        counter.put(operationNumber, count);
        prepared.get(operationNumber).set(backupIdx, 1);
    }

    boolean isBehind(int msgViewNumber, int msgOperationNumber) {
        return msgViewNumber < viewNumber || msgOperationNumber < operationNumber;
    }

    void addToLog(int clientID, int requestNumber, String operation) {
        log.addToLog(clientID, requestNumber, operation);
    }
    /*
    private String invokeOperation(String operation) {
        return log.invokeOperation(operation);
    }
    */
    private void replyToClient(int clientID, String operation, VREvent event) {
        clientResult.put(clientID, operation);
        commitNumber++;

        List<String> args = new ArrayList<>();
        args.add("reply");
        args.add(Integer.toString(viewNumber));
        args.add(Integer.toString(clientID));
        args.add(clientResult.get(clientID));

        output.add(new VREvent(args, event.socket));
    }

    boolean isNormal() {
        return (state == State.NORMAL);
    }

    String getLogAfter(int operationNumber) {
        return log.getLogAfter(operationNumber);
    }

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


    VRStatus(VRConfiguration conf, int x) throws IOException {
        configuration = conf;
        log = new VRLog();
        idx = x;
        nodes = new Socket[configuration.n];

        for (int i = 0; i < configuration.n; i++) {
            nodes[i] = new Socket(InetAddress.getByName(configuration.address[idx]), configuration.port[idx]);

            try {
                nodes[i].connect(new InetSocketAddress(InetAddress.getByName(configuration.address[i]),
                        configuration.port[i]), 200);
                messageProcessing(nodes[i]);
            } catch (IOException ignored) {
                nodes[i] = null;
            }
        }
    }

    private static final Map<String , Integer> ARGS = new HashMap<>() {{
        put("request", 3);
        put("prepare", 6);
        put("prepareOK", 3);
        put("reply", 3);
        put("commit", 2);
        put("getstate", 3);
        put("newstate", 4);
        put("startviewchange", 2);
        put("doviewchange", 6);
        put("startview", 4);
        put("ping", 1);
    }};
}
