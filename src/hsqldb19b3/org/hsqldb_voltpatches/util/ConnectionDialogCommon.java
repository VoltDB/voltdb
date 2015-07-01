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


package org.hsqldb_voltpatches.util;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.util.Enumeration;
import java.util.Hashtable;

import org.hsqldb_voltpatches.lib.java.JavaSystem;

// sqlbob@users 20020407 - patch 1.7.0 - reengineering
// fredt@users - 20040508 - modified patch by lonbinder@users for saving settings
// weconsultants@users - 20041114 - patch 1.8.0 - Added MySQL Connector/J jbcDriver and granulated imports

/**
 * Common code in the Swing and AWT versions of ConnectionDialog
 *
 * New class based on Hypersonic original
 *
 * @author Thomas Mueller (Hypersonic SQL Group)
 * @version 1.7.2
 * @since 1.7.0
 */
class ConnectionDialogCommon {

    private static String[][]       connTypes;
    private static final String[][] sJDBCTypes = {
        {
            "HSQL Database Engine In-Memory", "org.hsqldb_voltpatches.jdbcDriver",
            "jdbc:hsqldb:mem:."
        }, {
            "HSQL Database Engine Standalone", "org.hsqldb_voltpatches.jdbcDriver",
            "jdbc:hsqldb:file:\u00ABdatabase/path?\u00BB"
        }, {
            "HSQL Database Engine Server", "org.hsqldb_voltpatches.jdbcDriver",
            "jdbc:hsqldb:hsql://localhost/"
        }, {
            "HSQL Database Engine WebServer", "org.hsqldb_voltpatches.jdbcDriver",
            "jdbc:hsqldb:http://\u00ABhostname/?\u00BB"
        }, {
            "JDBC-ODBC Bridge from Sun", "sun.jdbc.odbc.JdbcOdbcDriver",
            "jdbc:odbc:\u00ABdatabase?\u00BB"
        }, {
            "Cloudscape RMI", "RmiJdbc.RJDriver",
            "jdbc:rmi://\u00ABhost?\u00BB:1099/jdbc:cloudscape:"
            + "\u00ABdatabase?\u00BB;create=true"
        }, {
            "IBM DB2", "COM.ibm.db2.jdbc.app.DB2Driver",
            "jdbc:db2:\u00ABdatabase?\u00BB"
        }, {
            "IBM DB2 (thin)", "COM.ibm.db2.jdbc.net.DB2Driver",
            "jdbc:db2://\u00ABhost?\u00BB:6789/\u00ABdatabase?\u00BB"
        }, {
            "Informix", "com.informix.jdbc.IfxDriver",
            "jdbc:informix-sqli://\u00ABhost?\u00BB:1533/\u00ABdatabase?\u00BB:"
            + "INFORMIXSERVER=\u00ABserver?\u00BB"
        }, {
            "InstantDb", "jdbc.idbDriver", "jdbc:idb:\u00ABdatabase?\u00BB.prp"
        }, {
            "MySQL Connector/J", "com.mysql.jdbc.Driver",
            "jdbc:mysql://\u00ABhost?\u00BB/\u00ABdatabase?\u00BB"
        }, {
            "MM.MySQL", "org.gjt.mm.mysql.Driver",
            "jdbc:mysql://\u00ABhost?\u00BB/\u00ABdatabase?\u00BB"
        }, {
            "Oracle", "oracle.jdbc.driver.OracleDriver",
            "jdbc:oracle:oci8:@\u00ABdatabase?\u00BB"
        }, {
            "Oracle (thin)", "oracle.jdbc.driver.OracleDriver",
            "jdbc:oracle:thin:@\u00ABhost?\u00BB:1521:\u00ABdatabase?\u00BB"
        }, {
            "PointBase", "com.pointbase.jdbc.jdbcUniversalDriver",
            "jdbc:pointbase://\u00ABhost?\u00BB/\u00ABdatabase?\u00BB"
        }, {
            "PostgreSQL", "org.postgresql.Driver",
            "jdbc:postgresql://\u00ABhost?\u00BB/\u00ABdatabase?\u00BB"
        }, {
            "PostgreSQL v6.5", "postgresql.Driver",
            "jdbc:postgresql://\u00ABhost?\u00BB/\u00ABdatabase?\u00BB"
        }
    };

    static String[][] getTypes() {

        return sJDBCTypes;
/*

        if (connTypes == null) {


            // Pluggable connection types:
            Vector plugTypes = new Vector();

            try {
                plugTypes = (Vector) Class.forName(
                    System.getProperty(
                        "org.hsqldb_voltpatches.util.ConnectionTypeClass")).newInstance();
            } catch (Exception e) {
                ;
            }

            connTypes =
                new String[(plugTypes.size() / 3) + sJDBCTypes.length][3];

            int i = 0;

            for (int j = 0; j < plugTypes.size(); i++) {
                connTypes[i]    = new String[3];
                connTypes[i][0] = plugTypes.elementAt(j++).toString();
                connTypes[i][1] = plugTypes.elementAt(j++).toString();
                connTypes[i][2] = plugTypes.elementAt(j++).toString();
            }

            for (int j = 0; j < sJDBCTypes.length; i++, j++) {
                connTypes[i]    = new String[3];
                connTypes[i][0] = sJDBCTypes[j][0];
                connTypes[i][1] = sJDBCTypes[j][1];
                connTypes[i][2] = sJDBCTypes[j][2];
            }
        }

        return (connTypes);
 */
    }

    private static final String fileName       = "hsqlprefs.dat";
    private static File         recentSettings = null;

    static Hashtable loadRecentConnectionSettings() throws IOException {

        Hashtable list = new Hashtable();

        try {
            if (recentSettings == null) {
                setHomeDir();

                if (homedir == null) {
                    return list;
                }

                recentSettings = new File(homedir, fileName);

                if (!recentSettings.exists()) {
                    JavaSystem.createNewFile(recentSettings);

                    // Changed back to what I recived from you
//                  recentSettings.createNewFile();
                    return list;
                }
            }
        } catch (Throwable e) {
            return list;
        }

        FileInputStream   in        = null;
        ObjectInputStream objStream = null;

        try {
            in        = new FileInputStream(recentSettings);
            objStream = new ObjectInputStream(in);

            list.clear();

            while (true) {
                ConnectionSetting setting =
                    (ConnectionSetting) objStream.readObject();

                if (!emptySettingName.equals(setting.getName())) {
                    list.put(setting.getName(), setting);
                }
            }
        } catch (EOFException eof) {

            // reached end of file -- this is not clean but it works
        } catch (ClassNotFoundException cnfe) {
            throw (IOException) new IOException("Unrecognized class type "
                                                + cnfe.getMessage());
        } catch (ClassCastException cce) {
            throw (IOException) new IOException("Unrecognized class type "
                                                + cce.getMessage());
        } catch (Throwable t) {}
        finally {
            if (objStream != null) {
                objStream.close();
            }

            if (in != null) {
                in.close();
            }
        }

        return list;
    }

    static String emptySettingName = "Recent settings...";

    /**
     * Adds the new settings name if it does not nexist, or overwrites the old one.
     */
    static void addToRecentConnectionSettings(Hashtable settings,
            ConnectionSetting newSetting) throws IOException {
        settings.put(newSetting.getName(), newSetting);
        ConnectionDialogCommon.storeRecentConnectionSettings(settings);
    }

    /**
     * Here's a non-secure method of storing recent connection settings.
     *
     * @param settings ConnectionSetting[]
     * @throw IOException if something goes wrong while writing
     */
    private static void storeRecentConnectionSettings(Hashtable settings) {

        try {
            if (recentSettings == null) {
                setHomeDir();

                if (homedir == null) {
                    return;
                }

                recentSettings = new File(homedir, fileName);

                if (!recentSettings.exists()) {

//                    recentSettings.createNewFile();
                }
            }

            if (settings == null || settings.size() == 0) {
                return;
            }

            // setup a stream to a physical file on the filesystem
            FileOutputStream   out = new FileOutputStream(recentSettings);
            ObjectOutputStream objStream = new ObjectOutputStream(out);
            Enumeration        en        = settings.elements();

            while (en.hasMoreElements()) {
                objStream.writeObject(en.nextElement());
            }

            objStream.flush();
            objStream.close();
            out.close();
        } catch (Throwable t) {}
    }

    /**
     * Removes the recent connection settings file store.
     */
    static void deleteRecentConnectionSettings() {

        try {
            if (recentSettings == null) {
                setHomeDir();

                if (homedir == null) {
                    return;
                }

                recentSettings = new File(homedir, fileName);
            }

            if (!recentSettings.exists()) {
                recentSettings = null;

                return;
            }

            recentSettings.delete();

            recentSettings = null;
        } catch (Throwable t) {}
    }

    private static String homedir = null;

    public static void setHomeDir() {

//#ifdef JAVA2FULL
        if (homedir == null) {
            try {
                Class c =
                    Class.forName("sun.security.action.GetPropertyAction");
                Constructor constructor = c.getConstructor(new Class[]{
                    String.class });
                java.security.PrivilegedAction a =
                    (java.security.PrivilegedAction) constructor.newInstance(
                        new Object[]{ "user.home" });

                homedir =
                    (String) java.security.AccessController.doPrivileged(a);
            } catch (Exception e) {
                System.err.println(
                    "No access to home directory.  Continuing without...");
            }
        }

//#endif
    }
}
