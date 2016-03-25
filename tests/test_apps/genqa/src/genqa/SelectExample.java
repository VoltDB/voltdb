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

import java.sql.*;

public class SelectExample {
    public static void main(String[] args) {
        Connection conn;

        // in real test program, set these from command line options
        final String DRIVER = "com.vertica.jdbc.Driver";
        final String HOST_PORT = "volt15d:5433";
        final String DB = "Test1";
        final String USER = "dbadmin";
        final String PASSWORD = "";
        final String DBMS = "vertica";
        final String CONNECTSTRING = "jdbc:" + DBMS + "://" + HOST_PORT + "/"
                + DB;

        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            System.err.println("Could not find the JDBC driver class.\n");
            e.printStackTrace();
            return;
        }

        try {
            conn = DriverManager.getConnection(CONNECTSTRING, USER, PASSWORD);
            // So far so good, lets issue a query...
            Statement mySelect = conn.createStatement();
            ResultSet myResult = mySelect
                    .executeQuery("SELECT * FROM export_partitioned_table order by rowid limit 10");
            ResultSetMetaData metaData = myResult.getMetaData();

            while (myResult.next()) {
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    System.out.println(i + ": " + myResult.getString(i));
                }
            }
            mySelect.close();
            conn.close();
        } catch (SQLException e) {
            System.err.println("Could not connect to the database.\n");
            e.printStackTrace();
            return;
        }
    } // end of main method
} // end of class
