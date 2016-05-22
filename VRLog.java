package ru.ifmo;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class VRLog {
    private int idx;
    List<VRLogEntry> list = new ArrayList<>();
    Map<Integer, Integer> clientTable = new HashMap<>();
    Map<Integer, String> clientResult = new HashMap<>();
    private Map<String, String> storage = new HashMap<>();
    private String fileName;

    VRLog(int i) {
        idx = i;
        fileName = "dkvs" + Integer.toString(idx + 1) + ".log";
        clientTable = new HashMap<>();
        clientResult = new HashMap<>();


        try( FileReader fr = new FileReader(fileName);
             BufferedReader br = new BufferedReader(fr)) {
            int ch;
            String lines = "";
            while ((ch =  br.read()) != -1) {
                    lines += (char)ch;
            }
            appendLog(lines, true);
        } catch (IOException ignored) {}
    }

    String getLogAfter(int operationNumber) {
        String res = "";
        for (int i = operationNumber + 1; i < list.size(); i++) {
            res += list.get(i).string();
        }
        return res;
    }

    void addToLog(int clientID, int requestNumber, String operation) {
        clientTable.put(clientID, requestNumber);
        clientResult.remove(clientID);
        list.add(new VRLogEntry(clientID, requestNumber, operation));
    }

    String invokeOperation(int i) {
        i--;
        String op = list.get(i).operation;
        String key;
        String res;
        switch (op.substring(0, 3)) {
            case "get":
                key = op.substring(4);
                if (storage.containsKey(key))
                    res = "VALUE " + key + " " + storage.get(key);
                else
                    res = "NOT_FOUND";
                break;
            case "set":
                String arg = op.substring(4);
                int j = 0;
                while (arg.charAt(j) != ' ') {
                    j++;
                }
                storage.put(arg.substring(0, j), arg.substring(j + 1));
                res = "STORED";
                break;
            case "del":
                key = op.substring(7);
                if (storage.containsKey(key)) {
                    storage.remove(key);
                    res = "DELETED";
                } else {
                    res = "NOT_FOUND";
                }
                break;
            default:
                res = "PONG";
        }
        try( FileWriter fw = new FileWriter(fileName, true);
             BufferedWriter bw = new BufferedWriter(fw);
             PrintWriter out = new PrintWriter(bw)) {
            out.println(list.get(i).string());
        } catch (IOException ignored) {}
        if (clientTable.get(list.get(i).clientID) == list.get(i).requestNumber)
            clientResult.put(list.get(i).clientID, res);
        return res;
    }


    void appendLog(String newLog) {
        appendLog(newLog, false);
    }

    private void appendLog(String newLog, boolean endlines) {
        int i = 0;
        while (newLog.length() > i) {
            int st0 = i;
            while (newLog.charAt(i) != '_') {
                i++;
            }
            int st1 = i;
            i++;
            while (newLog.charAt(i) != '_') {
                i++;
            }
            int st2 = i;
            i++;
            while (newLog.charAt(i) != '_') {
                i++;
            }
            int st3 = i;

            int clientID = Integer.parseInt(newLog.substring(st0, st1));
            int requestNumber = Integer.parseInt(newLog.substring(st1 + 1, st2));
            int len = Integer.parseInt(newLog.substring(st2 + 1, st3));
            String operation = newLog.substring(st3 + 1, st3 + len + 1);
            addToLog(clientID, requestNumber, operation);
            i += len + 1;
            if (endlines)
                i++;
        }
    }

    void replace(String log) {
        list = new ArrayList<>();
        clientTable = new HashMap<>();
        clientResult = new HashMap<>();
        storage = new HashMap<>();
        appendLog(log);
    }

    void cutTo(int commitNumber) {
        while (list.size() != commitNumber) {
            list.remove(list.size() - 1);
        }
    }
}
