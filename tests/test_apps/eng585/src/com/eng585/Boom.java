package com.eng585;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;

import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.types.TimestampType;

public class Boom {
        static class MyAsyncCallback implements ProcedureCallback {
                @Override
                public synchronized void clientCallback(ClientResponse clientResponse) {
                        final byte status = clientResponse.getStatus();

                        if (status != ClientResponse.SUCCESS) {
                                System.err.println("Failed to execute!!!");
                                System.err.println(clientResponse.getStatusString());
                                System.err.println(clientResponse.getException());
                                System.exit(-1);
                        } else {
                            long max = clientResponse.getResults()[0].asScalarLong();
                            System.out.println("MAX: " + max);
                        }
                }
        }

        public static void main(String[] args) {
                org.voltdb.client.Client myApp = null;
                String hostname = null;
                String username = null;
                String password = null;
                try {

                        if ((args != null) && (args.length < 3)) {
                                System.out
                                                .println("args: hostname username password");
                                System.exit(1);

                        }
                        hostname = args[0];
                        username = args[1];
                        password = args[2];

                        myApp = getConnection(hostname, username, password);

                        myApp.callProcedure(new MyAsyncCallback(), "test", "1");

                } catch (Exception e) {
                        e.printStackTrace();
                        System.exit(-1);
                } finally {
                        try {
                                if (myApp != null) {
                                        myApp.drain();
                                        myApp.close();
                                }
                        } catch (NoConnectionsException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                        } catch (InterruptedException ie) {
                                // TODO Auto-generated catch block
                                ie.printStackTrace();
                        }
                }

        }

        static private org.voltdb.client.Client getConnection(String hostname,
                        String user, String password) throws Exception {
                org.voltdb.client.Client myApp = null;
                myApp = ClientFactory.createClient();
                myApp.createConnection(hostname, user, password);

                return myApp;

        }
}
