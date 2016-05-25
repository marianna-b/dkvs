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
    private int accessPrimary = 10;

    int changeView = 0;
    private int changeViewOld = 0;
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

    boolean checkUpToDate(int needView, int needOperation) {
        return needView <= viewNumber && needOperation <= operationNumber;
    }

    boolean isBehind(int msgViewNumber, int msgOperationNumber) {
        return msgViewNumber < viewNumber || msgOperationNumber < operationNumber;
    }

    void updateWith(String newLog, int newView, int newOpNumber, int newCommitNumber) {
        log.appendLog(newLog);
        viewNumber = newView;
        operationNumber = newOpNumber;
        commitToOperation(newCommitNumber);
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
