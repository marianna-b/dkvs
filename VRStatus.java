package ru.ifmo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class VRStatus {
    private enum State {
        NORMAL,
        VIEWCHANGE,
        RECOVERING
    }
    int idx;
    VRLog log;
    int amount;
    int timeout;
    VRCommunications comm;

    Map<Integer, Integer> clientTable = new HashMap<>();
    Map<Integer, String> clientResult = new HashMap<>();
    Map<Integer, VREvent> clients;

    Map<Integer, Integer> counter = new HashMap<>();
    Map<Integer, List<Integer> > prepared = new HashMap<>();

    int operationNumber = 0;
    int commitNumber = 0;
    int viewNumber = 0;
    private State state = State.NORMAL;
    boolean accessPrimary;

    int getPrimary() {
        return viewNumber % amount;
    }

    boolean isPrimary() {
        return (getPrimary() == idx);
    }

    boolean accessPrimary() {
        if (!accessPrimary)
            return false;
        else {
            accessPrimary = false;
            return true;
        }
    }

    private boolean isRecovering() {
        return (state == State.RECOVERING);
    }

    boolean isNormal() {
        return (state == State.NORMAL);
    }

    private void replaceState(int v, String l, int n, int k) {
        state = State.NORMAL;
        viewNumber = v;
        log.replace(l, k);
        operationNumber = n;
        commitNumber = k;
        accessPrimary = true;
        clientResult = new HashMap<>();
        clientTable = new HashMap<>();
    }

    private void startView(VREvent event) {
        if (isNormal())
            return;
        int v = Integer.getInteger(event.args.get(0));
        String l = event.args.get(1);
        int n = Integer.getInteger(event.args.get(2));
        int k = Integer.getInteger(event.args.get(3));

        replaceState(v, l, n, k);
        for (int i = commitNumber + 1; i <= operationNumber; i++) {
            comm.sendPrepareOK(viewNumber, i, idx, getPrimary());
        }
    }

    private void newState(VREvent event) {
        int v = Integer.parseInt(event.args.get(0));
        String l = event.args.get(1);
        int n = Integer.parseInt(event.args.get(2));
        int k = Integer.parseInt(event.args.get(3));

        if (isBehind(v, n))
            return;

        if (isRecovering())
            updateWith(l, v, n, k);
    }

    void updateTO(int needView, int needOperation) {
        state = State.RECOVERING;
        int i = idx;
        while (operationNumber < needOperation || needView > viewNumber) {
            i = (i + 1) % amount;
            comm.sendGetState(viewNumber, operationNumber, idx, i);
            newState(comm.waitForType("newstate"));
        }
        state = State.NORMAL;
    }

    // TODO cleanup
    private int change_view = 0;
    private int change_view_old = 0;
    private int change_operation = 0;
    private int change_commit = 0;
    private String change_log = "";


    // TODO cleanup
    void startViewChange(VREvent event) {
        state = State.VIEWCHANGE;
        int[] list = new int[amount];
        int count = 0;
        if (isPrimary()) {
            if (event.type.equals("doviewchange")) {
                count++;
                int i = Integer.parseInt(event.args.get(5));
                list[i] = 1;

                change_view  = Integer.parseInt(event.args.get(0));
                change_log = event.args.get(1);
                change_view_old  = Integer.parseInt(event.args.get(2));
                change_operation  = Integer.parseInt(event.args.get(3));
                change_commit  = Integer.parseInt(event.args.get(4));
            }

        } else {
            if (event.type.equals("startviewchange")) {
                count++;
                int i = Integer.parseInt(event.args.get(1));
                list[i] = 1;
            }
        }
        while (count <= amount / 2) {
            if (isPrimary()) {
                VREvent curr = comm.waitForType("doviewchange");

                int v  = Integer.parseInt(curr.args.get(0));
                String l = curr.args.get(1);
                int v_old  = Integer.parseInt(curr.args.get(2));
                int n  = Integer.parseInt(curr.args.get(3));
                int k  = Integer.parseInt(curr.args.get(4));
                int i  = Integer.parseInt(curr.args.get(5));

                if (list[i] == 0) {
                    list[i]++;
                    count++;
                }

                updateChangeVal(v, l, v_old, n, k);
            } else {
                VREvent curr = comm.waitForType("startviewchange");
                int i  = Integer.parseInt(curr.args.get(1));
                if (list[i] == 0) {
                    list[i]++;
                    count++;
                }
            }
        }
        if (isPrimary()) {
            comm.sendStartView(change_view, change_log, change_operation, change_commit);
        } else {
            int newView = Integer.parseInt(event.args.get(0));
            String logString = log.getLogAfter(-1);
            int newPrimary = newView % amount;
            comm.sendDoViewChange(newView, logString, viewNumber, operationNumber, commitNumber, idx, newPrimary);

            startView(comm.waitForType("startview"));
        }
        state = State.NORMAL;
    }

    // TODO cleanup
    private void updateChangeVal(int v, String l, int v_old, int n, int k) {
        if (v_old < change_view_old)
            return;
        if (v_old == change_view_old && n < change_operation)
            return;
        change_operation = n;
        change_commit = k;
        change_view_old = v_old;
        change_view = v;
        change_log = l;
    }

    boolean checkUpToDate(int needView, int needOperation) {
        return needView <= viewNumber && needOperation <= operationNumber;
    }

    boolean isBehind(int msgViewNumber, int msgOperationNumber) {
        return msgViewNumber < viewNumber || msgOperationNumber < operationNumber;
    }

    private void updateWith(String newLog, int newView, int newOpNumber, int newCommitNumber) {
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

    void setupCounter(int operationNumber) {
        counter.put(operationNumber, 0);
        List<Integer> l = new ArrayList<>();
        for (int i = 0; i < amount; i++) {
            l.add(0);
        }
        prepared.put(operationNumber, l);
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


    void addToLog(int clientID, int requestNumber, String operation) {
        log.addToLog(clientID, requestNumber, operation);
    }

    String getLogAfter(int operationNumber) {
        return log.getLogAfter(operationNumber);
    }

    VRStatus(VRConfiguration conf, int x) throws IOException {
        comm = new VRCommunications(conf, x);
        timeout = conf.timeout;
        amount = conf.n;
        log = new VRLog();
        idx = x;
    }

}
