/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package org.hsqldb_voltpatches;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLWarning;
import java.sql.Statement;

import junit.framework.TestCase;

public class TestJDBC extends TestCase {

    /*public void testSimpleStuff() {
        String ddl = "create table test (cash integer default 23);";
        String dml = "insert into test values (123);";
        String query = "select * from test;";

        Connection dbconn;

        try {
            Class.forName("org.hsqldb_voltpatches.jdbcDriver" );

            dbconn = DriverManager.getConnection("jdbc:hsqldb:mem:x1", "sa", "");
            dbconn.setAutoCommit(true);
            dbconn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

            Statement stmt = dbconn.createStatement();
            stmt.execute(ddl);
            SQLWarning warn = stmt.getWarnings();
            if (warn != null)
                System.out.println("warn: " + warn.getMessage());
            assertTrue(warn == null);

            long ucount = stmt.executeUpdate(dml);
            assertTrue(ucount == 1);

            ResultSet rs = stmt.executeQuery(query);
            assertTrue(rs != null);
            ResultSetMetaData rsmd = rs.getMetaData();
            assertTrue(rsmd != null);
            assertTrue(rsmd.getColumnCount() == 1);

            boolean success = rs.next();
            assertTrue(success);

            int x = rs.getInt(1);
            assertTrue(x == 123);

            try {
                stmt.execute("SHUTDOWN;");
            } catch (Exception e) {};
            dbconn.close();
            System.gc();

        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }*/

    public void testVarbinary() {
        String ddl = "create table testvb (cash integer default 23, b varbinary(1024) default NULL);";
        String dml = "insert into testvb values (123, 'AAAA');";
        String query = "select * from testvb;";

        Connection dbconn;

        try {
            Class.forName("org.hsqldb_voltpatches.jdbcDriver" );

            dbconn = DriverManager.getConnection("jdbc:hsqldb:mem:x1", "sa", "");
            dbconn.setAutoCommit(true);
            dbconn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

            Statement stmt = dbconn.createStatement();
            stmt.execute(ddl);
            SQLWarning warn = stmt.getWarnings();
            if (warn != null)
                System.out.println("warn: " + warn.getMessage());
            assertTrue(warn == null);

            long ucount = stmt.executeUpdate(dml);
            assertTrue(ucount == 1);

            ResultSet rs = stmt.executeQuery(query);
            assertTrue(rs != null);
            ResultSetMetaData rsmd = rs.getMetaData();
            assertTrue(rsmd != null);
            assertTrue(rsmd.getColumnCount() == 2);

            boolean success = rs.next();
            assertTrue(success);

            int x = rs.getInt(1);
            assertTrue(x == 123);

            try {
                stmt.execute("SHUTDOWN;");
            } catch (Exception e) {};
            dbconn.close();
            System.gc();

        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }

    public void testDecimal() {
        String ddl = "create table test (cash decimal default 23.587);";
        String dml = "insert into test values (123.45678911111);";
        String query = "select * from test;";

        Connection dbconn;

        try {
            Class.forName("org.hsqldb_voltpatches.jdbcDriver" );

            dbconn = DriverManager.getConnection("jdbc:hsqldb:mem:x1", "sa", "");
            dbconn.setAutoCommit(true);
            dbconn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

            Statement stmt = dbconn.createStatement();
            stmt.execute(ddl);
            SQLWarning warn = stmt.getWarnings();
            if (warn != null)
                System.out.println("warn: " + warn.getMessage());
            assertTrue(warn == null);

            long ucount = stmt.executeUpdate(dml);
            assertTrue(ucount == 1);

            ResultSet rs = stmt.executeQuery(query);
            assertTrue(rs != null);
            ResultSetMetaData rsmd = rs.getMetaData();
            assertTrue(rsmd != null);
            assertTrue(rsmd.getColumnCount() == 1);

            /*System.out.printf("Typename %s, Type %d, Precision %s, Scale %d, Classname %s\n",
                    rsmd.getColumnTypeName(1),
                    rsmd.getColumnType(1),
                    rsmd.getPrecision(1),
                    rsmd.getScale(1),
                    rsmd.getColumnClassName(1));*/

            boolean success = rs.next();
            assertTrue(success);

            BigDecimal x = rs.getBigDecimal(1);
            assertNotNull(x);
            //System.out.printf("Value: %.10f\n", x.doubleValue());
            BigDecimal expected = new BigDecimal(123.4567);
            assertTrue(x.subtract(expected).abs().doubleValue() < .01);

            try {
                stmt.execute("SHUTDOWN;");
            } catch (Exception e) {};
            dbconn.close();
            System.gc();

        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }

    public void testTinyInt() {
        String ddl = "create table test (cash tinyint default 0);";
        String dml = "insert into test values (123);";
        String query = "select * from test;";

        Connection dbconn;

        try {
            Class.forName("org.hsqldb_voltpatches.jdbcDriver" );

            dbconn = DriverManager.getConnection("jdbc:hsqldb:mem:x1", "sa", "");
            dbconn.setAutoCommit(true);
            dbconn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

            Statement stmt = dbconn.createStatement();
            stmt.execute(ddl);
            SQLWarning warn = stmt.getWarnings();
            if (warn != null)
                System.out.println("warn: " + warn.getMessage());
            assertTrue(warn == null);

            long ucount = stmt.executeUpdate(dml);
            assertTrue(ucount == 1);

            ResultSet rs = stmt.executeQuery(query);
            assertTrue(rs != null);
            ResultSetMetaData rsmd = rs.getMetaData();
            assertTrue(rsmd != null);
            assertTrue(rsmd.getColumnCount() == 1);

            /*System.out.printf("Typename %s, Type %d, Precision %s, Scale %d, Classname %s\n",
                    rsmd.getColumnTypeName(1),
                    rsmd.getColumnType(1),
                    rsmd.getPrecision(1),
                    rsmd.getScale(1),
                    rsmd.getColumnClassName(1));*/

            boolean success = rs.next();
            assertTrue(success);

            int x = rs.getInt(1);
            assertTrue(x == 123);

            try {
                stmt.execute("SHUTDOWN;");
            } catch (Exception e) {};
            dbconn.close();
            System.gc();

        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }
}
