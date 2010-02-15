package org.hsqldb;

import java.math.BigDecimal;
import java.sql.*;
import java.sql.Statement;
import junit.framework.TestCase;

public class TestJDBC extends TestCase {

    public void testSimpleStuff() {
        String ddl = "create table test (cash integer default 23);";
        String dml = "insert into test values (123);";
        String query = "select * from test;";

        Connection dbconn;

        try {
            Class.forName("org.hsqldb.jdbcDriver" );

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
    }

    public void testDecimal() {
        String ddl = "create table test (cash decimal default 23.587);";
        String dml = "insert into test values (123.45678911111);";
        String query = "select * from test;";

        Connection dbconn;

        try {
            Class.forName("org.hsqldb.jdbcDriver" );

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
            Class.forName("org.hsqldb.jdbcDriver" );

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
