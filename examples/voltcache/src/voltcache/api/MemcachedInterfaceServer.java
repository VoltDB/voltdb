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
package voltcache.api;

import java.net.ServerSocket;
import org.voltdb.client.exampleutils.AppHelper;

public class MemcachedInterfaceServer
{
   public static void main (String[] args)
    {
        try
        {

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Use the AppHelper utility class to retrieve command line application parameters

            // Define parameters and pull from command line
            AppHelper apph = new AppHelper(MemcachedInterfaceServer.class.getCanonicalName())
                .add("mport", "memcached_port_number", "Port against which the interface will listen for connection of Memcache clients.", 11211)
                .add("vservers", "comma_separated_voltdb_server_list", "List of VoltDB servers to connect to.", "localhost")
                .add("vport", "voltdb_port_number", "Client port to connect to on VoltDB nodes.", 21212)
                .setArguments(args)
            ;

            // Retrieve parameters
            String vservers = apph.stringValue("vservers");
            int vport       = apph.intValue("vport");
            int mport       = apph.intValue("mport");

            // Display actual parameters, for reference
            apph.printActualUsage();

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            final ServerSocket socket        = new ServerSocket(mport);

            // Successfully created Server Socket. Now wait for connections.
            try
            {
                // Should really be using NIO, but the translation interface is bound to have cost no matter what
                for(;;)
                    (new Thread(new MemcachedTextProtocolService(socket.accept(), vservers, vport))).start();
            }
            catch(Exception x)
            {
                System.out.println("Exception encountered on accept. Shutting Down.");
                x.printStackTrace();
            }
            finally
            {
                try { socket.close(); } catch(Exception cx) {}
            }
        }
        catch (Exception x)
        {
            System.out.println("Could not initialize Memcache Interface Server.");
            x.printStackTrace();
            System.exit(-1);
        }
    }
}

