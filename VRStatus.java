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
    VRCommunications comm;

    Map<Integer, VREvent> clients;

    Map<Integer, Integer> counter = new HashMap<>();
    Map<Integer, List<Integer> > prepared = new HashMap<>();

    int operationNumber;
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

    private boolean isPrimary(int v) {
        return (v % amount) == idx;
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

    private boolean isNormal() {
        return (state == State.NORMAL);
    }

    private void replaceState(int v, String l, int n, int k) {
        viewNumber = v;
        commitNumber = 0;
        log.replace(l);
        commitToOperation(k);
        operationNumber = n;
        commitNumber = k;
        accessPrimary = true;
        log.clientResult = new HashMap<>();
        log.clientTable = new HashMap<>();
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
        while (operationNumber < needOperation || viewNumber < needView) {
            i = (i + 1) % amount;
            comm.sendGetState(viewNumber, operationNumber, idx, i);
            newState(comm.waitForType("newstate"));
        }
        state = State.NORMAL;
    }

    void startViewChange(VREvent event) {
        state = State.VIEWCHANGE;
        int[] list = new int[amount];
        int count = 0;

        int v = Integer.parseInt(event.args.get(0));

        int changeView = 0;
        int changeViewOld = 0;
        int changeOperation = 0;
        int changeCommit = 0;
        String changeLog = "";

        if (isPrimary(v)) {
            if (event.type.equals("doviewchange")) {
                count++;
                int i = Integer.parseInt(event.args.get(5));
                list[i] = 1;

                changeView  = Integer.parseInt(event.args.get(0));
                changeLog = event.args.get(1);
                changeViewOld  = Integer.parseInt(event.args.get(2));
                changeOperation  = Integer.parseInt(event.args.get(3));
                changeCommit  = Integer.parseInt(event.args.get(4));
            }

        } else {
            if (event.type.equals("startviewchange")) {
                count++;
                int i = Integer.parseInt(event.args.get(1));
                list[i] = 1;
            }
        }
        while (count <= amount / 2) {
            if (isPrimary(v)) {
                VREvent curr = comm.waitForType("doviewchange");

                int v_old  = Integer.parseInt(curr.args.get(2));
                int n  = Integer.parseInt(curr.args.get(3));
                int i  = Integer.parseInt(curr.args.get(5));

                if (list[i] == 0) {
                    list[i]++;
                    count++;
                }

                if (v_old < changeViewOld)
                    continue;

                if (v_old == changeViewOld && n < changeOperation)
                    continue;

                changeOperation = n;
                changeCommit = Integer.parseInt(curr.args.get(4));
                changeViewOld = v_old;
                changeView = Integer.parseInt(curr.args.get(0));
                changeLog = curr.args.get(1);

            } else {
                VREvent curr = comm.waitForType("startviewchange");
                int i  = Integer.parseInt(curr.args.get(1));
                if (list[i] == 0) {
                    list[i]++;
                    count++;
                }
            }
        }
        if (isPrimary(v)) {
            comm.sendStartView(changeView, changeLog, changeOperation, changeCommit, idx);
        } else {
            int newView = Integer.parseInt(event.args.get(0));
            String logString = log.getLogAfter(-1);
            int newPrimary = newView % amount;
            comm.sendDoViewChange(newView, logString, viewNumber, operationNumber, commitNumber, idx, newPrimary);

            startView(comm.waitForType("startview"));
        }
        state = State.NORMAL;
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
        amount = conf.n;
        log = new VRLog(x);
        operationNumber = log.list.size();
        idx = x;
    }
}
