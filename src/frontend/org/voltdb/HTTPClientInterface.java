/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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

package org.voltdb;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

import org.voltdb.client.AuthenticatedConnectionCache;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.SyncCallback;
import org.voltdb.utils.Encoder;

public class HTTPClientInterface {

    AuthenticatedConnectionCache m_connections = null;
    MessageDigest m_md = null;
    static final int CACHE_TARGET_SIZE = 10;

    public HTTPClientInterface() {
        try {
            m_md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("JVM doesn't support SHA-1 hashing. Please use a supported JVM", e);
        }
    }

    public String process(String uri, String method, Properties header, Properties parms) {
        String msg;

        Client client = null;

        try {
            if (m_connections == null) {
                int port = VoltDB.instance().getConfig().m_port;
                m_connections = new AuthenticatedConnectionCache(10, "localhost", port);
            }

            String username = parms.getProperty("User");
            String password = parms.getProperty("Password");
            String hashedPassword = parms.getProperty("Hashedpassword");
            String procName = parms.getProperty("Procedure");
            String params = parms.getProperty("Parameters");

            // The SHA-1 hash of the password
            byte[] hashedPasswordBytes = null;

            if (password != null) {
                try {
                    hashedPasswordBytes = m_md.digest(password.getBytes("UTF-8"));
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException("JVM doesn't support UTF-8. Please use a supported JVM", e);
                }
            }
            // note that HTTP Var "Hashedpassword" has a higher priority
            // Hashedassword must be a 40-byte hex-encoded SHA-1 hash (20 bytes unencoded)
            if (hashedPassword != null) {
                if (hashedPassword.length() != 40) {
                    throw new Exception("Hashedpassword must be a 40-byte hex-encoded SHA-1 hash (20 bytes unencoded).");
                }
                try {
                    hashedPasswordBytes = Encoder.hexDecode(hashedPassword);
                }
                catch (Exception e) {
                    throw new Exception("Hashedpassword must be a 40-byte hex-encoded SHA-1 hash (20 bytes unencoded).");
                }
            }

            assert((hashedPasswordBytes == null) || (hashedPasswordBytes.length == 20));

            // get a connection to localhost from the pool
            client = m_connections.getClient(username, hashedPasswordBytes);

            SyncCallback scb = new SyncCallback();
            boolean success;

            if (params != null) {
                ParameterSet paramSet = ParameterSet.fromJSONString(params);
                success =  client.callProcedure(scb, procName, paramSet.toArray());
            }
            else {
                success = client.callProcedure(scb, procName);
            }
            if (!success) {
                throw new Exception("Server is not accepting work at this time.");
            }

            scb.waitForResponse();

            ClientResponseImpl rimpl = (ClientResponseImpl) scb.getResponse();
            msg = rimpl.toJSONString();
        }
        catch (Exception e) {
            msg = e.getMessage();
            ClientResponseImpl rimpl = new ClientResponseImpl(ClientResponse.UNEXPECTED_FAILURE, new VoltTable[0], msg);
            //e.printStackTrace();
            msg = rimpl.toJSONString();
        }
        finally {
            if (client != null) {
                assert(m_connections != null);
                m_connections.releaseClient(client);
            }
        }

        return msg;
    }
}
