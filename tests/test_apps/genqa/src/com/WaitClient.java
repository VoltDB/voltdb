/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
package com;

import java.util.Random;

import org.voltdb.client.Client;
import org.voltdb.*;

public class WaitClient
{
    // Prints application usage.
    public static void printUsage()
    {
        System.out.println(
          "Usage: WaitClient --help\n"
                        + "   or  WaitClient [--wait=wait_duration]\n"
        + "                  [--multi]\n"
        + "                  [--repeat=total_number_of_calls]\n"
        + "                  [--servers=comma_separated_server_list]\n"
        + "                  [--port=port_number]\n"
        + "\n"
        + "[--wait=wait_duration]\n"
        + "  Wait duration (in seconds).\n"
        + "  Default: 5s.\n"
        + "\n"
        + "--multi\n"
        + "  Whether wait operation should be multi-partition (by default performs Single-partition waits).\n"
        + "\n"
        + "[--repeat=total_number_of_calls]\n"
        + "  Total number of calls.\n"
        + "  Default: 100 calls.\n"
        + "\n"
        + "[--servers=comma_separated_server_list]\n"
        + "  List of servers to connect to.\n"
        + "  Default: localhost.\n"
        + "\n"
        + "[--port=port_number]\n"
        + "  Client port to connect to on cluster nodes.\n"
        + "  Default: 21212.\n"
        );
    }

    public static void main(String args[])
    {
        try
        {
            // Initialize parameter defaults
            String procedure = "WaitSinglePartition";
            long wait = 5000l;
            int repeat = 100;
            String serverList = "localhost";
            int port = 21212;

            Random rand = new Random();

            // Parse out parameters
            for(int i = 0; i < args.length; i++)
            {
                String arg = args[i];
                if (arg.startsWith("--wait="))
                    wait = 1000l*Long.valueOf(arg.split("=")[1]);
                else if (arg.startsWith("--repeat="))
                    repeat = Integer.valueOf(arg.split("=")[1]);
                else if (arg.equals("--multi"))
                    procedure = "WaitMultiPartition";
                else if (arg.startsWith("--servers="))
                    serverList = arg.split("=")[1];
                else if (arg.startsWith("--port="))
                    port = Integer.valueOf(arg.split("=")[1]);
                else if (arg.equals("--help"))
                {
                    printUsage();
                    System.exit(0);
                }
                else
                {
                    System.err.println("Invalid Usage.");
                    printUsage();
                    System.exit(-1);
                }
            }

            // Validate parameters
            if ((repeat <= 0) || (wait <= 0))
            {
                System.err.println("Invalid Usage.");
                printUsage();
                System.exit(-1);
            }

            // Split server list
            String[] servers = serverList.split(",");

            // Print out parameters
            System.out.printf(
                               "-------------------------------------------------------------------------------------\n"
                             + "       Procedure: %s\n"
                             + "            Wait: %d ms\n"
                             + "          Repeat: %d times\n"
                             + "         Servers: %s\n"
                             + "            Port: %d\n"
                             + "-------------------------------------------------------------------------------------\n"
                             , procedure
                             , wait
                             , repeat
                             , serverList
                             , port
                             );

            // Create connection
            Client client = ClientExtensions.GetClient(servers,port);

            for(int i = 0; i < repeat; i++)
            {
                // Note slight 10 ms wait jitter to help spread around single-partition waits
                System.out.printf("%d / %d\n", i+1, repeat);
                System.out.flush();
                client.callProcedure(procedure, wait + ((rand.nextInt(10)*rand.nextInt(10)) % 10));
            }

            // Close client
            client.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}

