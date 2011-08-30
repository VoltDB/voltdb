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

package org.voltdb.jdbc;

import java.sql.*;
import java.util.*;
import java.util.regex.*;
import org.voltdb.*;
import org.voltdb.client.*;
import org.voltdb.client.exampleutils.*;

public class Driver implements java.sql.Driver
{
    //Driver URL prefix.
    private static final String URL_PREFIX = "jdbc:voltdb:";

    private static final int MAJOR_VERSION = 1;
    private static final int MINOR_VERSION = 0;

    static
    {
        try
        {
            DriverManager.registerDriver(new Driver());
        }
        catch(Exception e)
        {}
    }

    public Driver() throws SQLException
    {
        // Required for Class.forName().newInstance()
    }

    public Connection connect(String url, Properties info) throws SQLException
    {
        if(acceptsURL(url))
        {
            try
            {
                Matcher m = Pattern.compile("^jdbc:voltdb://([^:]+):(\\d+)$", Pattern.CASE_INSENSITIVE).matcher(url);
                if (m.matches())
                {
                    String[] servers = m.group(1).split(",");
                    int port = Integer.parseInt(m.group(2));
                    String user = "";
                    String password = "";
                    boolean heavyweight = false;
                    int maxoutstandingtxns = 0;
                    for (Enumeration e = info.propertyNames(); e.hasMoreElements();)
                    {
                        String key = (String) e.nextElement();
                        String value = info.getProperty(key);
                        if (key.toLowerCase().equals("user"))
                            user = value;
                        else if (key.toLowerCase().equals("password"))
                            password = value;
                        else if (key.toLowerCase().equals("heavyweight"))
                            heavyweight = (value.toLowerCase().equals("true") || value.toLowerCase().equals("yes") || value.toLowerCase().equals("1"));
                        else if (key.toLowerCase().equals("maxoutstandingtxns"))
                            maxoutstandingtxns = Integer.parseInt(value);
                        // else - unknown; ignore
                    }

                    // Return JDBC connection wrapper for the client
                    return new JDBC4Connection(ClientConnectionPool.get(servers,port,user,password,heavyweight,maxoutstandingtxns), user);
                }
            }
            catch(Exception x)
            {
                throw SQLError.get(x, SQLError.CONNECTION_UNSUCCESSFUL);
            }
        }
        return null;
    }

    public boolean acceptsURL(String url) throws SQLException
    {
        return Pattern.compile("^jdbc:voltdb://.+", Pattern.CASE_INSENSITIVE).matcher(url).matches();
    }

    public int getMajorVersion()
    {
        return MAJOR_VERSION;
    }

    public int getMinorVersion()
    {
        return MINOR_VERSION;
    }

    public DriverPropertyInfo[] getPropertyInfo(String url, Properties loginProps) throws SQLException
    {
        return new DriverPropertyInfo[0];
    }

    public boolean jdbcCompliant()
    {
        return false;
    }
}
