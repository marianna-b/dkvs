package ru.ifmo;

import java.io.*;
import java.net.*;

class VRClient {
    private int viewNumber;
    private int clientID;
    private int requestNumber = 0;
    private VRConfiguration configuration;

    private VRClient(VRConfiguration conf, int c, int r) {
        clientID = c;
        viewNumber = 0;
        requestNumber = r;
        configuration = conf;
    }

    private String request(String op) throws IOException {
        requestNumber ++;
        while (true) {
            Socket client = new Socket();
            try {
                int i = viewNumber % configuration.n;
                client.connect(new InetSocketAddress(InetAddress.getByName(configuration.address[i]), configuration.port[i]), 200);
                OutputStream outToServer = client.getOutputStream();
                PrintStream printStream = new PrintStream(outToServer);
                printStream.println("request");
                printStream.println(op);
                printStream.println(Integer.toString(clientID));
                printStream.println(Integer.toString(requestNumber));

                InputStream inFromServer = client.getInputStream();
                InputStreamReader inputStreamReader = new InputStreamReader(inFromServer);
                BufferedReader reader = new BufferedReader(inputStreamReader);
                try {

                    String res = reader.readLine();
                    if (res.equals("reply")) {
                        res = reader.readLine();
                        if (Integer.parseInt(reader.readLine()) == clientID) {
                            viewNumber = Integer.parseInt(res);
                            return reader.readLine();
                        }
                    }
                } catch (IOException e) {
                    viewNumber++;
                }
            } catch (SocketTimeoutException e) {
                viewNumber++;
            }
        }
    }

    public static void main(String[] args) {
        try {
            VRConfiguration configuration = new VRConfiguration("dkvs.properties");

            VRClient client = new VRClient(configuration, Integer.parseInt(args[0]), Integer.parseInt(args[1]));
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            String line;
            try {
                while ((line = reader.readLine()) != null) {
                    System.out.println(client.request(line));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
