package ru.ifmo;

import java.io.IOException;

class VRServer {
    private VRStatus S;

    private void request(VREvent event) {
        String operation = event.args.get(0);
        int clientID = Integer.parseInt(event.args.get(1));
        int requestNumber = Integer.parseInt(event.args.get(2));

        if (!S.isPrimary()) {
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
            if (S.log.clientResult.get(clientID) == null)
                return;

            S.comm.replyToClient(clientID, event, S.viewNumber, S.log.clientResult.get(clientID));
            return;
        }

        S.operationNumber++;

        S.clients.put(S.operationNumber, event);

        S.addToLog(clientID, requestNumber, operation);

        S.setupCounter(S.operationNumber);
        S.comm.sendPrepare(operation, clientID, requestNumber, S.viewNumber, S.operationNumber, S.commitNumber, S.idx);
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

        if (S.isBehind(v, n - 1))
            return;

        if (S.isPrimary())
            return;

        if (S.commitNumber < k)
            S.commitToOperation(k);

        S.accessPrimary = true;

        S.operationNumber++;

        S.clients.put(S.operationNumber, event);

        S.addToLog(clientID, requestNumber, operation);

        S.comm.sendPrepareOK(S.viewNumber, S.operationNumber, S.idx, S.getPrimary());
    }

    private void prepareOK(VREvent event) {
        int v = Integer.parseInt(event.args.get(0));
        int n = Integer.parseInt(event.args.get(1));
        int i = Integer.parseInt(event.args.get(2));

        if (!S.checkUpToDate(v, n)) {
            S.updateTO(v, n);
            return;
        }

        if (S.isPrimary())
            S.addCounter(n, i);
    }

    private void getState(VREvent event) {
        int v = Integer.parseInt(event.args.get(0));
        int n = Integer.parseInt(event.args.get(1));
        int i = Integer.parseInt(event.args.get(2));

        if (S.viewNumber != v)
            return;
        S.comm.sendState(i, S.viewNumber, S.operationNumber, S.commitNumber, S.getLogAfter(n));
    }

    private void commit(VREvent event) {
        int v = Integer.parseInt(event.args.get(0));
        int k = Integer.parseInt(event.args.get(1));

        if (S.isBehind(v, k))
            return;

        if (!S.checkUpToDate(v, k))
            S.updateTO(v, k);

        if (S.isBehind(v, k))
            return;

        S.accessPrimary = true;
        S.commitToOperation(k);
    }

    private void ping() {
        if (S.isPrimary()) {
            S.comm.sendCommit(S.viewNumber, S.commitNumber, S.idx);
            return;
        }
        if (S.accessPrimary())
            return;

        VREvent self = S.comm.sendStartViewChange(S.viewNumber + 1, S.idx);
        viewChange(self);
    }

    private void viewChange(VREvent event) {
        int v = Integer.parseInt(event.args.get(0));
        if (S.viewNumber >= v)
            return;
        S.comm.sendStartViewChange(v, S.idx);
        S.startViewChange(event);
    }

    private void node(VREvent event) {
        int i = Integer.parseInt(event.args.get(0));
        S.comm.reconnect(i);
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
                break;
            case "doviewchange":
                viewChange(event);
                break;
            case "startviewchange":
                viewChange(event);
                break;
            case "newstate":
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
            case "node":
                node(event);
                break;
            default:
        }
    }

    private void checkAndCommit() {
        if (S.operationNumber == S.commitNumber)
            return;
        if (S.counter.get(S.commitNumber + 1) < S.amount / 2)
            return;
        S.counter.remove(S.commitNumber + 1);
        S.prepared.remove(S.commitNumber + 1);
        String res = S.log.invokeOperation(S.commitNumber + 1);
        VREvent event = S.clients.get(S.commitNumber + 1);

        int clientID = Integer.parseInt(event.args.get(1));
        S.log.clientResult.put(clientID, res);
        S.commitNumber++;

        S.comm.replyToClient(clientID, event, S.viewNumber, S.log.clientResult.get(clientID));
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
