/* Copyright (c) 2001-2009, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb_voltpatches.lib;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.StringTokenizer;

/* $Id: RCData.java 1153 2009-02-14 04:14:23Z unsaved $ */

/**
 * Manages all the details we need to connect up to database(s),
 * in a declarative way.
 * <P/>
 * The file <CODE>src/org/hsqldb_voltpatches/sample/SqlFileEmbedder.java</CODE>
 * in the HSQLDB distribution provides an example of how to use RCData for your
 * own programs.
 *
 * @see <a href="../../../../util-guide/sqltool-chapt.html#sqltool_auth-sect"
 *      target="guide">
 *     The RC File section of the HyperSQL Utilities Guide</a>
 * @see org.hsqldb_voltpatches.sample.SqlFileEmbedder
 * @author Blaine Simpson (blaine dot simpson at admc dot com)
 */
public class RCData {

    public static final String DEFAULT_JDBC_DRIVER   = "org.hsqldb_voltpatches.jdbc.JDBCDriver";
    private String             defaultJdbcDriverName = DEFAULT_JDBC_DRIVER;

    public void setDefaultJdbcDriver(String defaultJdbcDriverName) {
        this.defaultJdbcDriverName = defaultJdbcDriverName;
    }

    public String getDefaultJdbcDriverName() {
        return defaultJdbcDriverName;
    }

    /**
     * DISABLED DUE TO SECURITY CONCERNS.
     * Just for testing and debugging.
     *
     * N.b. this echoes passwords!
    public void report() {
        System.err.println("urlid: " + id + ", url: " + url + ", username: "
                           + username + ", password: " + password);
    }
     */

    /**
     * Creates a RCDataObject by looking up the given key in the
     * given authentication file.
     *
     * @param dbKey Key to look up in the file.
     *              If null, then will echo all urlids in the file to stdout.
     * @param file File containing the authentication information.
     */
    public RCData(File file, String dbKey) throws Exception {

        if (file == null) {
            throw new IllegalArgumentException("RC file name not specified");
        }

        if (!file.canRead()) {
            throw new IOException("Please set up authentication file '" + file
                                  + "'");
        }

        // System.err.println("Using RC file '" + file + "'");
        StringTokenizer tokenizer = null;
        boolean         thisone   = false;
        String          s;
        String          keyword, value;
        int             linenum = 0;
        BufferedReader  br      = new BufferedReader(new FileReader(file));

        while ((s = br.readLine()) != null) {
            ++linenum;

            s = s.trim();

            if (s.length() == 0) {
                continue;
            }

            if (s.charAt(0) == '#') {
                continue;
            }

            tokenizer = new StringTokenizer(s);

            if (tokenizer.countTokens() == 1) {
                keyword = tokenizer.nextToken();
                value   = "";
            } else if (tokenizer.countTokens() > 1) {
                keyword = tokenizer.nextToken();
                value   = tokenizer.nextToken("").trim();
            } else {
                try {
                    br.close();
                } catch (IOException e) {
                    // Can only report on so many errors at one time
                }

                throw new Exception("Corrupt line " + linenum + " in '" + file
                                    + "':  " + s);
            }

            if (dbKey == null) {
                if (keyword.equals("urlid")) {
                    System.out.println(value);
                }

                continue;
            }

            if (keyword.equals("urlid")) {
                if (value.equals(dbKey)) {
                    if (id == null) {
                        id      = dbKey;
                        thisone = true;
                    } else {
                        try {
                            br.close();
                        } catch (IOException e) {
                            // Can only report on so many errors at one time
                        }

                        throw new Exception("Key '" + dbKey + " redefined at"
                                            + " line " + linenum + " in '"
                                            + file);
                    }
                } else {
                    thisone = false;
                }

                continue;
            }

            if (thisone) {
                if (keyword.equals("url")) {
                    url = value;
                } else if (keyword.equals("username")) {
                    username = value;
                } else if (keyword.equals("driver")) {
                    driver = value;
                } else if (keyword.equals("charset")) {
                    charset = value;
                } else if (keyword.equals("truststore")) {
                    truststore = value;
                } else if (keyword.equals("password")) {
                    password = value;
                } else if (keyword.equals("transiso")) {
                    ti = value;
                } else if (keyword.equals("libpath")) {
                    libpath = value;
                } else {
                    try {
                        br.close();
                    } catch (IOException e) {
                        // Can only report on so many errors at one time
                    }

                    throw new Exception("Bad line " + linenum + " in '" + file
                                        + "':  " + s);
                }
            }
        }

        br.close();

        if (dbKey == null) {
            return;
        }

        if (url == null) {
            throw new Exception("url not set " + "for '" + dbKey
                                + "' in file '" + file + "'");
        }

        if (libpath != null) {
            throw new IllegalArgumentException(
                "Sorry, 'libpath' not supported yet");
        }
    }

    /**
     * Convenience constructor for backward compatibility.
     *
     * @see #RCData(String,String,String,String,String,String,String,String)
     */
    public RCData(String id, String url, String username, String password,
                  String driver, String charset,
                  String truststore) throws Exception {
        this(id, url, username, password, driver, charset, truststore, null);
    }

    /**
     * Wrapper for unset Transaction Isolation.
     */
    public RCData(String id, String url, String username, String password,
                  String driver, String charset, String truststore,
                  String libpath) throws Exception {
        this(id, url, username, password, driver, charset, truststore,
                libpath, null);
    }

    /**
     * <p>Creates a new <code>RCData</code> object.
     *
     * <p>The parameters driver, charset, truststore, and libpath are optional.
     * Setting these parameters to <code>NULL</code> will set them to their
     * default values.
     *
     * @param id The identifier for these connection settings
     * @param url The URL of the database to connect to
     * @param username The username to log in as
     * @param password The password of the username
     * @param driver The JDBC driver to use
     * @param charset The character set to use
     * @param truststore The trust store to use
     * @param libpath The JDBC library to add to CLASSPATH
     * @throws Exception if the a non-optional parameter is set to <code>NULL</code>
     */
    public RCData(String id, String url, String username, String password,
                  String driver, String charset, String truststore,
                  String libpath, String ti) throws Exception {

        this.id         = id;
        this.url        = url;
        this.username   = username;
        this.password   = password;
        this.ti         = ti;
        this.driver     = driver;
        this.charset    = charset;
        this.truststore = truststore;
        this.libpath    = libpath;

        if (libpath != null) {
            throw new IllegalArgumentException(
                "Sorry, 'libpath' not supported yet");
        }

        if (id == null || url == null) {
            throw new Exception("id or url was not set");
        }
    }

    /* Purposefully not using JavaBean paradigm so that these fields can
     * be used as a traditional, public DO */
    public String id         = null;
    public String url        = null;
    public String username   = null;
    public String password   = null;
    public String ti         = null;
    public String driver     = null;
    public String charset    = null;
    public String truststore = null;
    public String libpath    = null;

    /**
     * Gets a JDBC Connection using the data of this RCData object.
     *
     * @return New JDBC Connection
     */
    public Connection getConnection()
    throws ClassNotFoundException, SQLException, MalformedURLException {
        return getConnection(null, null, null);
    }

    /**
     * Gets a JDBC Connection using the data of this RCData object with
     * specified override elements
     *
     * @return New JDBC Connection
     */
    public Connection getConnection(String curDriverIn, String curCharsetIn,
                                    String curTrustStoreIn)
                                    throws ClassNotFoundException,
                                           MalformedURLException,
                                           SQLException {

        // Local vars to satisfy compiler warnings
        String curDriver = curDriverIn;
        String curCharset = curCharsetIn;
        String curTrustStore = curTrustStoreIn;

        Properties sysProps = System.getProperties();

        if (curDriver == null) {

            // If explicit driver not specified
            curDriver = ((driver == null) ? DEFAULT_JDBC_DRIVER
                                          : driver);
        }

        if (curCharset == null && charset != null) {
            curCharset = charset;
        }

        if (curTrustStore == null && truststore != null) {
            curTrustStore = truststore;
        }

        if (curCharset == null) {
            sysProps.remove("sqlfile.charset");
        } else {
            sysProps.put("sqlfile.charset", curCharset);
        }

        if (curTrustStore == null) {
            sysProps.remove("javax.net.ssl.trustStore");
        } else {
            sysProps.put("javax.net.ssl.trustStore", curTrustStore);
        }

        String urlString = null;

        try {
            urlString = expandSysPropVars(url);
        } catch (IllegalArgumentException iae) {
            throw new MalformedURLException(iae.getMessage() + " for URL '"
                                            + url + "'");
        }

        String userString = null;

        if (username != null) try {
            userString = expandSysPropVars(username);
        } catch (IllegalArgumentException iae) {
            throw new MalformedURLException(iae.getMessage()
                                            + " for user name '" + username
                                            + "'");
        }

        String passwordString = null;

        if (password != null) try {
            passwordString = expandSysPropVars(password);
        } catch (IllegalArgumentException iae) {
            throw new MalformedURLException(iae.getMessage()
                                            + " for password");
        }

        Class.forName(curDriver);
        // This is not necessary for jdbc:odbc or if class loaded by a
        // service resource file.  Consider checking for that.

        Connection c = (userString == null)
                     ? DriverManager.getConnection(urlString)
                     : DriverManager.getConnection(urlString, userString,
                                                   passwordString);
        if (ti != null) RCData.setTI(c, ti);
        // Would like to verify the setting made by checking
        // c.getTransactionIsolation().  Unfortunately, the spec allows for
        // databases to substitute levels according to some rules, and it's
        // impossible to know what to expect since custom levels are permitted.
        // Debug:
        // System.err.println("TI set to " + ti + "\nPOST: "
        // + SqlTool.tiToString(c.getTransactionIsolation()));

        return c;
    }

    static public String expandSysPropVars(String inString) {

        String outString = new String(inString);
        int    varOffset, varEnd;
        String varVal, varName;

        while (true) {

            // Recursive substitution for ${x} variables.
            varOffset = outString.indexOf("${");

            if (varOffset < 0) {
                break;
            }

            varEnd = outString.indexOf('}', varOffset + 2);

            if (varEnd < 0) {
                break;
            }

            varName = outString.substring(varOffset + 2, varEnd);

            if (varName.length() < 1) {
                throw new IllegalArgumentException("Bad variable setting");
            }

            varVal = System.getProperty(varName);

            if (varVal == null) {
                throw new IllegalArgumentException(
                    "No Java system property with name '" + varName + "'");
            }

            outString = outString.substring(0, varOffset) + varVal
                        + outString.substring(varEnd + 1);
        }

        return outString;
    }

    static public void setTI(Connection c, String tiString)
            throws SQLException {
        int i = -1;
        if (tiString.equals("TRANSACTION_READ_UNCOMMITTED"))
            i = Connection.TRANSACTION_READ_UNCOMMITTED;
        if (tiString.equals("TRANSACTION_READ_COMMITTED"))
            i = Connection.TRANSACTION_READ_COMMITTED;
        if (tiString.equals("TRANSACTION_REPEATABLE_READ"))
            i = Connection.TRANSACTION_REPEATABLE_READ;
        if (tiString.equals("TRANSACTION_SERIALIZABLE"))
            i = Connection.TRANSACTION_SERIALIZABLE;
        if (tiString.equals("TRANSACTION_NONE"))
            i = Connection.TRANSACTION_NONE;
        if (i < 0) {
            throw new SQLException(
                    "Trans. isol. value not supported by "
                    + RCData.class.getName() + ": " + tiString);
        }
        c.setTransactionIsolation(i);
    }

    /**
     * Return String for numerical java.sql.Connection Transaction level.
     *
     * Returns null, since DB implementations are free to provide
     * their own transaction isolation levels.
     */
    static public String tiToString(int ti) {
        switch (ti) {
            case Connection.TRANSACTION_READ_UNCOMMITTED:
                return "TRANSACTION_READ_UNCOMMITTED";
            case Connection.TRANSACTION_READ_COMMITTED:
                return "TRANSACTION_READ_COMMITTED";
            case Connection.TRANSACTION_REPEATABLE_READ:
                return "TRANSACTION_REPEATABLE_READ";
            case Connection.TRANSACTION_SERIALIZABLE:
                return "TRANSACTION_SERIALIZABLE";
            case Connection.TRANSACTION_NONE:
                return "TRANSACTION_NONE";
        }
        return "Custom Transaction Isolation numerical value: " + ti;
    }
}
