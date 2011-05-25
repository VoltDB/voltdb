/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.exportclient;

import org.voltdb.export.ExportProtoMessage.AdvertisedDataSource;

/**
 * An export client that pulls data and acks it back, but
 * never does anythign with it.
 *
 */
public class DiscardingExportClient extends ExportClientBase {

    public DiscardingExportClient(boolean useAdminPorts) {
        super(useAdminPorts);
    }

    static class DiscardDecoder extends ExportDecoderBase {

        public DiscardDecoder(AdvertisedDataSource source) {
            super(source);
        }

        @Override
        public boolean processRow(int rowSize, byte[] rowData) {
            return true;
        }

        @Override
        public void sourceNoLongerAdvertised(AdvertisedDataSource source) {
        }
    }

    @Override
    public ExportDecoderBase constructExportDecoder(AdvertisedDataSource source) {
        return new DiscardDecoder(source);
    }

    protected static void printHelpAndQuit(int code) {
        System.out
                .println("java -cp <classpath> org.voltdb.exportclient.DiscardingExportClient "
                        + "--help");
        System.out
                .println("java -cp <classpath> org.voltdb.exportclient.DiscardingExportClient "
                        + "--servers server1[,server2,...,serverN] "
                        + "--connect (admin|client) "
                        + "[--user export_username] "
                        + "[--password export_password]");
        System.out.println("Note that server hostnames may be appended with a specific port:");
        System.out.println("  --servers server1:port1[,server2:port2,...,serverN:portN]");

        System.exit(code);
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        String[] volt_servers = null;
        String user = null;
        String password = null;
        char connect = ' '; // either ' ', 'c' or 'a'

        for (int ii = 0; ii < args.length; ii++) {
            String arg = args[ii];
            if (arg.equals("--help")) {
                printHelpAndQuit(0);
            }
            else if (arg.equals("--connect")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --connect");
                    printHelpAndQuit(-1);
                }
                String connectStr = args[ii + 1];
                if (connectStr.equalsIgnoreCase("admin")) {
                    connect = 'a';
                } else if (connectStr.equalsIgnoreCase("client")) {
                    connect = 'c';
                } else {
                    System.err.println("Error: --type must be one of \"admin\" or \"client\"");
                    printHelpAndQuit(-1);
                }
                ii++;
            }
            else if (arg.equals("--servers")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --servers");
                    printHelpAndQuit(-1);
                }
                volt_servers = args[ii + 1].split(",");
                ii++;
            }
            else if (arg.equals("--user")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --user");
                    printHelpAndQuit(-1);
                }
                user = args[ii + 1];
                ii++;
            }
            else if (arg.equals("--password")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --password");
                    printHelpAndQuit(-1);
                }
                password = args[ii + 1];
                ii++;
            }
        }
        // Check args for validity
        if (volt_servers == null || volt_servers.length < 1) {
            System.err.println("DiscardingExportClient: must provide at least one VoltDB server");
            printHelpAndQuit(-1);
        }
        if (connect == ' ') {
            System.err.println("DiscardingExportClient: must specify connection type as admin or client using --connect argument");
            printHelpAndQuit(-1);
        }
        assert ((connect == 'c') || (connect == 'a'));
        if (user == null) {
            user = "";
        }
        if (password == null) {
            password = "";
        }

        // create the export to file client
        DiscardingExportClient client = new DiscardingExportClient(connect == 'a');

        // add all of the servers specified
        for (String server : volt_servers)
            client.addServerInfo(server, connect == 'a');

        // add credentials (default blanks used if none specified)
        client.addCredentials(user, password);

        // main loop
        try {
            client.run();
        }
        catch (ExportClientException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

}
