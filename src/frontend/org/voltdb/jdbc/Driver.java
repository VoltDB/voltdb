/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.jdbc;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class Driver implements java.sql.Driver
{
    public static final String JDBC_PROP_FILE_ENV = "VOLTDB_JDBC_PROPERTIES";
    public static final String JDBC_PROP_FILE_PROP = "voltdb.jdbcproperties";
    public static final String DEFAULT_PROP_FILENAME = "voltdb.properties";
    //Driver URL prefix.
    private static final String URL_PREFIX = "jdbc:voltdb:";

    // Static so it's unit-testable, yes, lazy me
    static String[] getServersFromURL(String url) {
        // get everything between the prefix and the ?
        String prefix = URL_PREFIX + "//";
        int end = url.length();
        if (url.indexOf("?") > 0) {
            end = url.indexOf("?");
        }
        String servstring = url.substring(prefix.length(), end);
        return servstring.split(",");
    }

    static Map<String, String> getPropsFromURL(String url) {
        Map<String, String> results = new HashMap<String, String>();
        if (url.indexOf("?") > 0) {
            String propstring = url.substring(url.indexOf("?") + 1);
            String[] props = propstring.split("&");
            for (String prop : props) {
                if (prop.indexOf("=") > 0) {
                    String[] comps = prop.split("=");
                    results.put(comps[0], comps[1]);
                }
            }
        }
        return results;
    }

    private static final int MAJOR_VERSION = 1;
    private static final int MINOR_VERSION = 0;

    static
    {
        try
        {
            DriverManager.registerDriver(new Driver());
        }
        catch (Exception e)
        {}
    }

    public Driver() throws SQLException
    {
        // Required for Class.forName().newInstance()
    }

    @Override
    public Connection connect(String url, Properties props) throws SQLException
    {
        if (acceptsURL(url))
        {
            try
            {
                // Properties favored order:
                // 1) property file specified by env variable
                // 2) property file specified by system property
                // 3) property file with default name in same path as driver jar
                // 4) Properties specified in the URL
                // 5) Properties specified to getConnection() arg
                //
                Properties fileprops = tryToFindPropsFile();

                // Copy the provided properties so we don't muck with
                // the object the caller gave us.
                Properties info = (Properties) props.clone();
                String prefix = URL_PREFIX + "//";
                if (!url.startsWith(prefix)) {
                    throw SQLError.get(SQLError.ILLEGAL_ARGUMENT);
                }

                // get the server strings
                String[] servers = Driver.getServersFromURL(url);
                // get the props from the URL
                Map<String, String> urlprops = Driver.getPropsFromURL(url);
                for (Entry<String, String> e : urlprops.entrySet()) {
                    // Favor the URL over the provided props
                    info.setProperty(e.getKey(), e.getValue());
                }

                // Favor the file-specified properties over the other props
                for (Enumeration<?> e = fileprops.propertyNames(); e.hasMoreElements();)
                {
                    String key = (String) e.nextElement();
                    info.setProperty(key, fileprops.getProperty(key));
                }

                String user = "";
                String password = "";
                boolean heavyweight = false;
                int maxoutstandingtxns = 0;
                for (Enumeration<?> e = info.propertyNames(); e.hasMoreElements();)
                {
                    String key = (String) e.nextElement();
                    String value = info.getProperty(key);
                    if (key.toLowerCase().equals("user"))
                        user = value;
                    else if (key.toLowerCase().equals("password"))
                        password = value;
                    else if (key.toLowerCase().equals("heavyweight"))
                        heavyweight = (value.toLowerCase().equals("true") || value.toLowerCase().equals("yes") ||
                                value.toLowerCase().equals("1"));
                    else if (key.toLowerCase().equals("maxoutstandingtxns"))
                        maxoutstandingtxns = Integer.parseInt(value);
                    // else - unknown; ignore
                }

                // Return JDBC connection wrapper for the client
                return new JDBC4Connection(JDBC4ClientConnectionPool.get(servers, user, password,
                            heavyweight, maxoutstandingtxns),
                        info);
            } catch (Exception x) {
                throw SQLError.get(x, SQLError.CONNECTION_UNSUCCESSFUL);
            }
        }
        return null;
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException
    {
        return Pattern.compile("^jdbc:voltdb://.+", Pattern.CASE_INSENSITIVE).matcher(url).matches();
    }

    @Override
    public int getMajorVersion()
    {
        return MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion()
    {
        return MINOR_VERSION;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties loginProps) throws SQLException
    {
        return new DriverPropertyInfo[0];
    }

    @Override
    public boolean jdbcCompliant()
    {
        return false;
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    private Properties tryToFindPropsFile() {
        Properties fileprops = new Properties();
        String filename = null;
        // Check the env first
        filename = System.getenv(Driver.JDBC_PROP_FILE_ENV);
        if (filename == null) {
            filename = System.getProperty(Driver.JDBC_PROP_FILE_PROP);
        }
        if (filename == null) {
            // see if we can find a file in the default location
            URL pathToJar = this.getClass().getProtectionDomain()
                    .getCodeSource().getLocation();
            String tmp = null;
            try {
                tmp = new File(pathToJar.toURI()).getParent() + File.separator + DEFAULT_PROP_FILENAME;
            } catch (Exception e) {
                tmp = null;
            }
            filename = tmp;
        }
        if (filename != null) {
            File propfile = new File(filename);
            if (propfile.exists() && propfile.isFile()) {
                FileInputStream in = null;
                try {
                    in = new FileInputStream(propfile);
                    fileprops.load(in);
                }
                catch (FileNotFoundException fnfe) {}
                catch (IOException ioe) {}
                finally {
                    if (in != null) {
                        try {
                            in.close();
                        } catch (IOException e) {}
                    }
                }
            }
        }

        return fileprops;
    }
}
