package ru.ifmo;

import java.io.IOException;
import java.io.PrintStream;
import java.net.Socket;
import java.util.List;

class VREvent {
    String type;
    List<String> args;
    Socket socket;
    int idx = -1;

    VREvent(List<String> strings, Socket s, int i) {
        type = strings.get(0);
        args = strings.subList(1, strings.size());
        socket = s;
        idx = i;
    }

    public String toString() {
        String res = type;
        for (String arg : args) {
            res += "\n" + arg;
        }
        return res;
    }

    void send() throws IOException {
        final PrintStream printStream = new PrintStream(socket.getOutputStream());
        printStream.println(toString());
    }
}
