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
package org.voltdb.clientutils;

import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;

public class ClientExtensions
{

    public static Client GetClient(String[] servers, int port)
    {
        final ClientConfig config = new ClientConfig("", "");
        final Client client = ClientFactory.createClient(config);

        for (String server : servers)
        {
            try
            {
                client.createConnection(server.trim(), port);
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
        return client;
    }

}
