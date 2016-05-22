package ru.ifmo;

class VRLogEntry {
    int clientID;
    int requestNumber;
    String operation;


    VRLogEntry(int c, int r, String op) {
        clientID = c;
        requestNumber = r;
        operation = op;
    }

    String string(){
        return Integer.toString(clientID) + "_" + Integer.toString(requestNumber) + "_"
                + Integer.toString(operation.length()) + "_" + operation;
    }
}
