package ru.ifmo;

import java.io.IOException;

class VRServer {
    private VRStatus S;

    private void request(VREvent event) {
        if (!S.isNormal())
            return;

        String operation = event.args.get(0);
        int clientID = Integer.parseInt(event.args.get(1));
        int requestNumber = Integer.parseInt(event.args.get(2));

        System.out.println("Received request clientID: " + Integer.toString(clientID) + " reqNum: " +
                    Integer.toString(requestNumber) + " operation: " + operation);

        if (!S.isPrimary()) {
            System.out.println("Because not primary close connection");
            try {
                event.socket.close();
            } catch (IOException ignored) {}
            return;
        }

        if (!S.log.clientTable.containsKey(clientID))
            S.log.clientTable.put(clientID, 0);
        if (S.log.clientTable.get(clientID) > requestNumber)
            return;
        if (S.log.clientTable.get(clientID) == requestNumber) {
            System.out.println("Resend result");
            if (S.log.clientResult.get(clientID) != null) {
                S.comm.replyToClient(clientID, event, S.viewNumber, S.log.clientResult.get(clientID));
                return;
            } else {
                return;
            }
        }

        S.operationNumber++;
        S.clients.put(S.operationNumber, event);
        S.addToLog(clientID, requestNumber, operation);
        S.setupCounter(S.operationNumber);
        S.comm.sendPrepare(operation, clientID, requestNumber, S.viewNumber, S.operationNumber, S.commitNumber, S.idx);
    }

    private void prepare(VREvent event) {
        if (!S.isNormal())
            return;
        int v = Integer.parseInt(event.args.get(0));
        String operation = event.args.get(1);
        int n = Integer.parseInt(event.args.get(2));
        int k = Integer.parseInt(event.args.get(3));
        int clientID = Integer.parseInt(event.args.get(3));
        int requestNumber = Integer.parseInt(event.args.get(4));

        System.out.println("Received prepare for view: " + Integer.toString(v) + " operation: " + Integer.toString(n) +
                    " operation: " + operation);

        if (S.isBehind(v, n - 1)) {
            System.out.println("Drop because too old");
            return;
        }

        S.resetAccessPrimary();
        if (!S.checkUpToDate(v, n - 1))
            if (!recoverTo(v, n - 1))
                return;

        if (S.isPrimary()) {
            System.out.println("Drop because primary doesn't prepare");
            return;
        }

        if (S.commitNumber < k)
            S.commitToOperation(k);

        S.operationNumber++;
        S.clients.put(S.operationNumber, event);
        S.addToLog(clientID, requestNumber, operation);
        S.comm.sendPrepareOK(S.viewNumber, S.operationNumber, S.idx, S.getPrimary());
    }

    private void prepareOK(VREvent event) {
        if (!S.isNormal())
            return;

        int v = Integer.parseInt(event.args.get(0));
        int n = Integer.parseInt(event.args.get(1));
        int i = Integer.parseInt(event.args.get(2));

        System.out.println("Received prepareOK for view: " + Integer.toString(v) + " operation: " + Integer.toString(n)
                    + " from: " + Integer.toString(i));

        if (!S.isPrimary()) {
            System.out.println("Drop because not primary");
            return;
        }

        S.addCounter(n, i);
    }

    private void getState(VREvent event) {
        if (!S.isNormal())
            return;
        int v = Integer.parseInt(event.args.get(0));
        int n = Integer.parseInt(event.args.get(1));
        int i = Integer.parseInt(event.args.get(2));

        System.out.println("Received getState for view: " + Integer.toString(v));

        if (S.viewNumber != v) {
            System.out.println("Drop because not same view");
            return;
        }
        S.comm.sendState(i, S.viewNumber, S.operationNumber, S.commitNumber, S.getLogAfter(n));
    }

    private void commit(VREvent event) {
        int v = Integer.parseInt(event.args.get(0));
        int k = Integer.parseInt(event.args.get(1));
        System.out.println("Received commit for view: " + Integer.toString(v) + " to commit to: " + Integer.toString(k));

        if (S.isBehind(v, k)) {
            System.out.println("Drop commit because too old");
            return;
        }

        S.resetAccessPrimary();

        if (S.isNormal()) {
            if (!S.checkUpToDate(v, k))
                if (!recoverTo(v, k))
                    return;

            if (S.isPrimary()) {
                System.out.println("Drop because primary");
                return;
            }
            S.commitToOperation(k);
        }
    }

    private int recover(VREvent event, int view, int op) {
        switch (event.type) {
            case "request":
                request(event);
                return 0;

            case "prepare":
                prepare(event);
                return 0;

            case "prepareOK":
                prepareOK(event);
                return 0;

            case "startview":
                startView(event);
                return 1;

            case "doviewchange":
                doViewChange(event);
                return -1;

            case "startviewchange":
                startViewChange(event);
                return -1;

            case "newstate":
                newState(event);
                if (S.viewNumber == view || S.operationNumber == op)
                    return 1;
                return 0;

            case "getstate":
                getState(event);
                return 0;

            case "commit":
                commit(event);
                return 0;

            case "ping":
                ping();
                if (S.isViewChange())
                    return -1;
                return 0;
            default:
                return 0;
        }
    }

    private boolean recoverTo(int view, int opNum) {
        System.out.println("Start recovering to view: " + Integer.toString(view) + " opNum " + Integer.toString(opNum));
        S.state = VRStatus.State.RECOVERING;
        S.resetAccessPrimary();
        if (view != S.viewNumber) {
            S.log.cutTo(S.commitNumber);
            S.operationNumber = S.commitNumber;
        }
        int op = S.operationNumber;

        S.comm.sendGetState(view, op, S.idx);
        while (true) {
            int res = recover(S.comm.getEvent(), view, op);
            if (res == 1)
                return true;
            if (res == -1)
                return false;
        }
    }

    private void startView(VREvent event) {
        int v = Integer.parseInt(event.args.get(0));
        int n = Integer.parseInt(event.args.get(2));

        System.out.println("Received startView with view: " + Integer.toString(v) + " opNum: " + Integer.toString(n));
        if (S.isBehind(v, n)) {
            System.out.println("Drop because too old");
            return;
        }
        S.resetAccessPrimary();
        System.out.println("Back to normal");
        S.state = VRStatus.State.NORMAL;
        S.startView(event);
    }

    private void ping() {
        System.out.println("ping");
        if (S.isPrimary() && S.isNormal()) {
            S.comm.sendCommit(S.viewNumber, S.commitNumber, S.idx);
            return;
        }
        if (S.isRecovering()) {
            return;
        }
        if (S.accessPrimary())
            return;


        if (S.isNormal()) {
            System.out.println("Timeout expired for primary, send startViewChange to all");
            S.setChange(S.viewNumber + 1, S.getLogAfter(-1), S.viewNumber, S.operationNumber, S.commitNumber);
            S.state = VRStatus.State.VIEWCHANGE;
            S.comm.sendStartViewChange(S.viewNumber + 1, S.idx);
        } else {
            //if ()
            //VREvent self = S.comm.sendStartViewChange(S.changeView + 1, S.idx);
            //startViewChange(self);
        }
    }

    private void newState(VREvent event) {
        if (!S.isRecovering()) {
            System.out.println("Drop because not recovering");
            return;
        }
        int v = Integer.parseInt(event.args.get(0));
        int n = Integer.parseInt(event.args.get(2));

        if (S.isBehind(v, n)) {
            System.out.println("Drop because too old");
            return;
        }

       if (S.isRecovering()) {
           S.newState(event);
       }
    }

    private void startViewChange(VREvent event) {
        int v = Integer.parseInt(event.args.get(0));
        int i = Integer.parseInt(event.args.get(1));
        System.out.println("Received srartViewChange to view: " + Integer.toString(v));

        if (S.viewNumber >= v) {
            System.out.println("Drop because too old");
            return;
        }

        // TODO is ok?
        if (v % S.amount == i)
            S.resetAccessPrimary();

        if (S.isNormal() || S.isRecovering()) {
            S.setChange(v, S.getLogAfter(-1), S.viewNumber, S.operationNumber, S.commitNumber);
            S.state = VRStatus.State.VIEWCHANGE;

            S.comm.sendStartViewChange(v, S.idx);
        }

        if (S.changeView < v) {
            S.restartChange(v);
        }

        if (S.startViewChangeList.get(i) != 0)
            return;

        S.counterStartViewChange++;
        S.startViewChangeList.set(i, 1);

        if (S.counterStartViewChange < S.amount / 2)
            return;

        String logString = S.log.getLogAfter(-1);
        int newPrimary = S.changeView % S.amount;
        if (newPrimary == S.idx) {
            S.counterDoViewChange++;
            S.doViewChangeList.set(S.idx, 1);
            return;
        }
        S.comm.sendDoViewChange(S.changeView, logString, S.viewNumber, S.operationNumber, S.commitNumber, S.idx, newPrimary);
    }

    private void doViewChange(VREvent event) {
        int v = Integer.parseInt(event.args.get(0));
        String l = event.args.get(1);
        int v_old  = Integer.parseInt(event.args.get(2));
        int n  = Integer.parseInt(event.args.get(3));
        int k = Integer.parseInt(event.args.get(4));
        int i = Integer.parseInt(event.args.get(5));

        System.out.println("Received doViewChange to view: " + Integer.toString(v));
        if (S.viewNumber >= v) {
            System.out.println("Drop because too old");
            return;
        }

        // TODO is ok?
        if (v % S.amount == i)
            S.resetAccessPrimary();

        if (S.isNormal() || S.isRecovering()) {
            S.state = VRStatus.State.VIEWCHANGE;
            S.setChange(v, S.getLogAfter(-1), S.viewNumber, S.operationNumber, S.commitNumber);
            S.updateChange(v, l, v_old, n, k);

            S.counterDoViewChange++;
            S.doViewChangeList.set(i, 1);
            S.comm.sendStartViewChange(v, S.idx);
            return;
        }
        if (v > S.changeView)
            S.setChange(v, l, v_old, n, k);
        else
            S.updateChange(v, l, v_old, n, k);

        if (S.doViewChangeList.get(i) != 0)
            return;

        S.counterDoViewChange++;
        S.doViewChangeList.set(i, 1);

        if (S.counterDoViewChange <= S.amount / 2)
            return;

        if (S.isPrimary(S.changeView)) {
            S.replaceState(S.changeView, S.changeLog, S.changeOperation, S.changeCommit);
            S.comm.sendStartView(S.changeView, S.changeLog, S.changeOperation, S.changeCommit, S.idx);
            S.state = VRStatus.State.NORMAL;
            System.out.println("Back to Normal");
        }
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
                ping();
                break;
            default:
        }
    }


    private void checkAndCommit() {
        if (S.isNormal()) {
            //System.out.println("Check if can commit");
            if (S.operationNumber == S.commitNumber)
                return;
            if (!S.counter.containsKey(S.commitNumber + 1))
                return;
            if (S.counter.get(S.commitNumber + 1) < S.amount / 2)
                return;
            System.out.println("Can commit operation " + Integer.toString(S.commitNumber + 1));
            S.counter.remove(S.commitNumber + 1);
            S.prepared.remove(S.commitNumber + 1);
            String res = S.log.invokeOperation(S.commitNumber + 1);
            VREvent event = S.clients.get(S.commitNumber + 1);

            int clientID = Integer.parseInt(event.args.get(1));
            S.log.clientResult.put(clientID, res);
            S.commitNumber++;

            S.comm.replyToClient(clientID, event, S.viewNumber, S.log.clientResult.get(clientID));
        }
    }

    private void main() {
        while (true) {
            if (S.isPrimary())
                checkAndCommit();
            eventProcess(S.comm.getEvent());
        }
    }

    VRServer(VRConfiguration conf, int x) throws IOException {
        S = new VRStatus(conf, x);
        main();
    }
}
