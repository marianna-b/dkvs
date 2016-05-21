package ru.ifmo;

import java.io.IOException;
import java.io.PrintStream;
import java.net.Socket;
import java.util.List;

/**
 * Created by mariashka on 5/18/16.
 */
class VREvent {
    String type;
    List<String> args;
    Socket socket;

    VREvent(List<String> strings, Socket s) {
        type = strings.get(0);
        args = strings.subList(1, strings.size());
        socket = s;
    }

    public String toString() {
        String res = type;
        for (String arg : args) {
            res += " " + arg;
        }
        return res;
    }

    void send() throws IOException {
        final PrintStream printStream = new PrintStream(socket.getOutputStream());
        printStream.print(toString());
    }
}
