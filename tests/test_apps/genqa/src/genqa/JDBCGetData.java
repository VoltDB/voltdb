/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

/* This example connects to Vertica and issues a simple select */

/*
 * To run:
 *   javac -cp /home/pshaw/voltdb/voltdb/voltdbclient-6.2.jar:/home/pshaw/voltdb/lib/vertica-jdbc.jar SelectExample.java
 *   java -cp /home/pshaw/voltdb/voltdb/voltdbclient-6.2.jar:/home/pshaw/voltdb/lib/vertica-jdbc.jar:. SelectExample
 */

package genqa;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import genqa.VerifierUtils.Config;

public class JDBCGetData {
    static Connection conn;

    // in real test program, set these from command line options
    final static String DRIVER = "com.vertica.jdbc.Driver";
    final static String HOST_PORT = "volt15d:5433";
    final static String DB = "Test1";
    final static String USER = "dbadmin";
    final static String PASSWORD = "";
    final static String DBMS = "vertica";
    final static String CONNECTSTRING = "jdbc:" + DBMS + "://" + HOST_PORT + "/" + DB;

    public static Connection jdbcConnect(Config config) {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            System.err.println("Could not find the JDBC driver class.\n");
            e.printStackTrace();
            System.exit(-1);
        }

        try {
            conn = DriverManager.getConnection(CONNECTSTRING, USER, PASSWORD);
        } catch (SQLException e) {
            System.err.println("Could not connect to the database.\n");
            e.printStackTrace();
            System.exit(-1);
        }
        return conn;
    }

    static ResultSet jdbcRead(long rowid) {
        ResultSet rs = null;
        try {
            // conn = DriverManager.getConnection (CONNECTSTRING, USER,
            // PASSWORD);
            // So far so good, lets issue a query...
            Statement stmt = conn.createStatement();
            rs = stmt
                    .executeQuery("SELECT * FROM export_partitioned_table where rowid = "
                            + rowid);
            ResultSetMetaData metaData = rs.getMetaData();


//            while (rs.next()) {
//                for (int i = 1; i <= metaData.getColumnCount(); i++) {
//                    System.out.println(i + ": " + rs.getString(i));
//                }
//            }
            // stmt.close();
            // conn.close();
        } catch (SQLException e) {
            System.err.println("Exception in DB row access.\n");
            e.printStackTrace();
            System.exit(-1);
        }
        return rs;
    }
}
