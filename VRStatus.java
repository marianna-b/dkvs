package ru.ifmo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class VRStatus {
    List<Integer> startViewChangeList;
    int counterStartViewChange;
    List<Integer> doViewChangeList;
    int counterDoViewChange;
    public boolean notWaitPrimary = false;

    void restartChange(int view) {

        counterStartViewChange = 0;
        counterDoViewChange = 0;
        startViewChangeList = new ArrayList<>();
        doViewChangeList = new ArrayList<>();
        for (int i = 0; i < amount; i++) {
            startViewChangeList.add(0);
            doViewChangeList.add(0);
        }
        changeView = view;
    }


    void setChange(int v, String l, int v_old, int n, int k) {
        restartChange(v);
        changeOperation = n;
        changeCommit = k;
        changeViewOld = v_old;
        changeLog = l;
    }

    void updateChange(int v, String l, int v_old, int n, int k) {
        if (v_old < changeViewOld)
            return;
        if (v_old == changeViewOld && n < changeOperation)
            return;
        if (v_old == changeViewOld && n == changeOperation && k < changeCommit)
            return;

        changeOperation = n;
        changeCommit = k;
        changeViewOld = v_old;
        changeView = v;
        changeLog = l;
    }

    enum State {
        NORMAL,
        VIEWCHANGE,
        RECOVERING
    }
    int idx;
    VRLog log;
    int amount;
    VRCommunications comm;

    Map<Integer, VREvent> clients = new HashMap<>();
    Map<Integer, Integer> counter = new HashMap<>();
    Map<Integer, List<Integer> > prepared = new HashMap<>();

    int operationNumber;
    int commitNumber = 0;
    int viewNumber = 0;
    State state = State.NORMAL;
    int accessPrimary = 10;

    int changeView = 0;
    int changeViewOld = 0;
    int changeOperation = 0;
    int changeCommit = 0;
    String changeLog = "";

    int getPrimary() {
        return viewNumber % amount;
    }

    boolean isPrimary() {
        return (getPrimary() == idx);
    }

    boolean isPrimary(int v) {
        return (v % amount) == idx;
    }

    boolean accessPrimary() {
        if (accessPrimary == 0) {
            return false;
        } else {
            accessPrimary--;
            return true;
        }
    }

    void resetAccessPrimary() {
        if (accessPrimary < 3)
            accessPrimary = 3;
    }

    boolean isRecovering() {
        return (state == State.RECOVERING);
    }

    boolean isViewChange() {
        return (state == State.VIEWCHANGE);
    }

    boolean isNormal() {
        return (state == State.NORMAL);
    }

    void replaceState(int v, String l, int n, int k) {
        viewNumber = v;
        commitNumber = 0;
        log.replace(l);
        commitToOperation(k);
        operationNumber = n;
        commitNumber = k;
        resetAccessPrimary();
        log.clientResult = new HashMap<>();
        log.clientTable = new HashMap<>();
    }

    void startView(VREvent event) {
        System.out.println(event.args.size());
        int v = Integer.parseInt(event.args.get(0));
        String l = event.args.get(1);
        int n = Integer.parseInt(event.args.get(2));
        int k = Integer.parseInt(event.args.get(3));

        if (isBehind(v, n)) {
            System.out.println("Drop because too old");
            return;
        }

        replaceState(v, l, n, k);
        for (int i = commitNumber + 1; i <= operationNumber; i++) {
            comm.sendPrepareOK(viewNumber, i, idx, getPrimary());
        }
    }

    void newState(VREvent event) {
        System.out.println("Getting new state");
        int v = Integer.parseInt(event.args.get(0));
        String l = event.args.get(1);
        int n = Integer.parseInt(event.args.get(2));
        int k = Integer.parseInt(event.args.get(3));

        if (isBehind(v, n)) {
            System.out.println("Drop because too old");
            return;
        }

        if (isRecovering())
            updateWith(l, v, n, k);
    }
    /*
    void updateTO(int needView, int needOperation) {
        System.out.println("Start recovering to view: " + Integer.toString(needView) + " opNum " + Integer.toString(needOperation));
        state = State.RECOVERING;
        int i = idx;
        while (operationNumber < needOperation || viewNumber < needView) {
            i = (i + 1) % amount;
            comm.sendGetState(viewNumber, operationNumber, idx, i);
            newState(comm.waitForType("newstate"));
        }
        state = State.NORMAL;
    }
    */


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
        commitToOperation(operationNumber);
        idx = x;
    }
}
