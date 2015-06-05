/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.jdbc;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.ServerThread;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.VoltProjectBuilder.ProcedureInfo;
import org.voltdb.compiler.VoltProjectBuilder.RoleInfo;
import org.voltdb.compiler.VoltProjectBuilder.UserInfo;
import org.voltdb.utils.MiscUtils;
import org.voltdb_testprocs.regressionsuites.securityprocs.DoNothing1;
import org.voltdb_testprocs.regressionsuites.securityprocs.DoNothing2;
import org.voltdb_testprocs.regressionsuites.securityprocs.DoNothing3;

public class TestJDBCSecurityEnabled {
    private static final String TEST_XML = "jdbcsecurityenabledtest.xml";
    private static final String TEST_JAR = "jdbcsecurityenabledtest.jar";
    static String testjar;
    static ServerThread server;
    static Connection conn;
    static Connection myconn;
    static VoltProjectBuilder pb;

    public static final RoleInfo GROUPS[] = new RoleInfo[] {
        new RoleInfo("Group1", false, false, false, false, false, false),
        new RoleInfo("Group2", true, false, false, false, false, false),
        new RoleInfo("Group3", true, false, false, false, false, false),
        new RoleInfo("GroupWithAllProcPerm", false, false, false, false, false, true),
        new RoleInfo("GroupWithDefaultProcPerm", false, false, false, true, false, false),
        new RoleInfo("GroupWithoutDefaultProcPerm", false, false, false, false, false, false),
        new RoleInfo("GroupWithDefaultProcReadPerm", false, false, false, false, true, false)
    };

    public static final UserInfo[] USERS = new UserInfo[] {
        new UserInfo("admin", "password", new String[] {"AdMINISTRATOR"}),
        new UserInfo("user1", "password", new String[] {"Group1"}),
        new UserInfo("user2", "password", new String[] {"Group2"}),
        new UserInfo("user3", "password", new String[] {"Group3"}),
        new UserInfo("userWithDefaultUserPerm", "password", new String[] {"User"}),
        new UserInfo("userWithAllProcPerm", "password", new String[] {"GroupWithAllProcPerm"}),
        new UserInfo("userWithDefaultProcPerm", "password", new String[] {"GroupWithDefaultProcPerm"}),
        new UserInfo("userWithoutDefaultProcPerm", "password", new String[] {"groupWiThoutDefaultProcPerm"}),
        new UserInfo("userWithDefaultProcReadPerm", "password", new String[] {"groupWiThDefaultProcReadPerm"})
    };

    public static final ProcedureInfo[] PROCEDURES = {
        new ProcedureInfo(new String[0], DoNothing1.class),
        new ProcedureInfo(new String[] { "Group1" }, DoNothing2.class),
        new ProcedureInfo(new String[] { "Group1", "Group2" }, DoNothing3.class)
    };

    @BeforeClass
    public static void setUp() throws Exception {
        pb = new VoltProjectBuilder();
        final String ddl = "CREATE TABLE T("
                + "A1 INTEGER NOT NULL, "
                + "A2 DECIMAL, "
                + "A3 DECIMAL DEFAULT 0, "
                + "PRIMARY KEY(A1));";
        pb.addLiteralSchema(ddl);
        pb.addPartitionInfo("T", "A1");

        pb.addRoles(GROUPS);
        pb.addUsers(USERS);
        pb.addProcedures(PROCEDURES);
        // create a default admin account
        pb.setSecurityEnabled(true, true);

        boolean success = pb.compile(Configuration.getPathToCatalogForTest(TEST_JAR), 3, 1, 0);
        assert(success);
        MiscUtils.copyFile(pb.getPathToDeployment(), Configuration.getPathToCatalogForTest(TEST_XML));
        testjar = Configuration.getPathToCatalogForTest(TEST_JAR);

        // Set up ServerThread and Connection
        startServer();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        stopServer();
        File f = new File(testjar);
        f.delete();
    }

    private static void startServer() throws ClassNotFoundException,
    SQLException {
        server = new ServerThread(testjar, pb.getPathToDeployment(),
                BackendTarget.NATIVE_EE_JNI);
        server.start();
        server.waitForInitialization();

        Class.forName("org.voltdb.jdbc.Driver");
        conn = DriverManager.getConnection("jdbc:voltdb://localhost:21212", "defaultadmin", "admin");
        myconn = null;
    }

    private static void stopServer() throws SQLException {
        if (conn != null) {
            conn.close();
            conn = null;
        }
        if (myconn != null) {
            myconn.close();
            myconn = null;
        }
        if (server != null) {
            try {
                server.shutdown();
            } catch (InterruptedException e) { /* empty */
            }
            server = null;
        }
    }


    private void CloseUserConnection() throws SQLException {
        if (myconn != null) {
            myconn.close();
            myconn = null;
        }
    }


    private static Connection getJdbcConnection(String url, Properties props)
            throws Exception {
        Class.forName("org.voltdb.jdbc.Driver");
        return DriverManager.getConnection(url, props);
    }


    @Test
    public void testAuthentication(){
        Properties props = new Properties();
        boolean threw;
        // Test failed auth with wrong username
        props.setProperty("user", "wronguser");
        props.setProperty("password", "wrongpassword");
        threw = false;
        try {
            myconn = getJdbcConnection("jdbc:voltdb://localhost:21212", props);
            CloseUserConnection();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("Unable to establish"));
            threw = true;
        }
        assertTrue("Connection which should have failed did not.", threw);

        // Test failed auth with wrong password
        props.setProperty("user", "user1");
        props.setProperty("password", "wrongpassword");
        threw = false;
        try {
            myconn = getJdbcConnection("jdbc:voltdb://localhost:21212", props);
            CloseUserConnection();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("Unable to establish"));
            threw = true;
        }
        assertTrue("Connection which should have failed did not.", threw);

        // Test success
        props.setProperty("user", "user1");
        props.setProperty("password", "password");
        threw = false;
        try {
            myconn = getJdbcConnection("jdbc:voltdb://localhost:21212", props);
            CloseUserConnection();
        }catch (Exception e) {
            e.printStackTrace();
            fail("Connection creation shouldn't fail: " + e.getMessage());
        }
    }

    @Test
    public void testAdmin() throws Exception {
        // administrator can run every thing
        Properties props = new Properties();
        props.setProperty("user", "admin");
        props.setProperty("password", "password");
        myconn = getJdbcConnection("jdbc:voltdb://localhost:21212", props);

        //sysproc should pass
        assertTrue(execSysProc());

        // adhoc should pass
        assertTrue(execAdhocProc());
        assertTrue(execAdhocReadProc());

        //default proc should pass
        assertTrue(execDefaultProc());
        assertTrue(execDefaultReadProc());

        //all user proc should pass
        assertTrue(execUserProc("DoNothing1"));
        assertTrue(execUserProc("DoNothing2"));
        assertTrue(execUserProc("DoNothing3"));

        //close connection
        CloseUserConnection();
    }


    @Test
    public void testUser1() throws Exception{
        // user1 can only run User-Defined procedure 2 and 3
        Properties props = new Properties();
        props.setProperty("user", "user1");
        props.setProperty("password", "password");
        myconn = getJdbcConnection("jdbc:voltdb://localhost:21212", props);

        //sysproc should fail
        assertFalse(execSysProc());

        // adhoc should fail
        assertFalse(execAdhocProc());
        assertFalse(execAdhocReadProc());

        //default proc should fail
        assertFalse(execDefaultProc());
        assertFalse(execDefaultReadProc());

        //user proc DoNothing1 should fail
        //user proc DoNothing2 should pass
        //user proc DoNothing3 should pass
        assertFalse(execUserProc("DoNothing1"));
        assertTrue(execUserProc("DoNothing2"));
        assertTrue(execUserProc("DoNothing3"));

        //close connection
        CloseUserConnection();
    }


    @Test
    public void testUser2() throws Exception {
        // user2 can run adhoc procedure, default procedur and user proc 3
        Properties props = new Properties();
        props.setProperty("user", "user2");
        props.setProperty("password", "password");
        myconn = getJdbcConnection("jdbc:voltdb://localhost:21212", props);

        //sysproc should fail
        assertFalse(execSysProc());

        // adhoc should pass
        assertTrue(execAdhocProc());
        assertTrue(execAdhocReadProc());

        //default proc should pass
        assertTrue(execDefaultProc());
        assertTrue(execDefaultReadProc());

        //user proc DoNothing1 should fail
        //user proc DoNothing2 should fail
        //user proc DoNothing3 should pass
        assertFalse(execUserProc("DoNothing1"));
        assertFalse(execUserProc("DoNothing2"));
        assertTrue(execUserProc("DoNothing3"));

        //close connection
        CloseUserConnection();
    }


    @Test
    public void testUser3() throws Exception {
        // user3 can run adhoc procedure, default procedure
        // no user procedures can run
        Properties props = new Properties();
        props.setProperty("user", "user3");
        props.setProperty("password", "password");
        myconn = getJdbcConnection("jdbc:voltdb://localhost:21212", props);

        //sysproc should fail
        assertFalse(execSysProc());

        // adhoc should pass
        assertTrue(execAdhocProc());
        assertTrue(execAdhocReadProc());

        //default proc should pass
        assertTrue(execDefaultProc());
        assertTrue(execDefaultReadProc());

        //user proc DoNothing1 should fail
        //user proc DoNothing2 should fail
        //user proc DoNothing3 should fail
        assertFalse(execUserProc("DoNothing1"));
        assertFalse(execUserProc("DoNothing2"));
        assertFalse(execUserProc("DoNothing3"));

        //close connection
        CloseUserConnection();
    }

    @Test
    public void testUserWithDefaultUserPerms() throws Exception{
        // userWithDefaultUserPerms can run ad hoc and all user-defined permission
        Properties props = new Properties();
        props.setProperty("user", "userWithDefaultUserPerm");
        props.setProperty("password", "password");
        myconn = getJdbcConnection("jdbc:voltdb://localhost:21212", props);

        //sysproc should fail
        assertFalse(execSysProc());

        // adhoc should pass
        assertTrue(execAdhocProc());
        assertTrue(execAdhocReadProc());

        //default proc should pass
        assertTrue(execDefaultProc());
        assertTrue(execDefaultReadProc());

        //user proc DoNothing1 should pass
        //user proc DoNothing2 should pass
        //user proc DoNothing3 should pass
        assertTrue(execUserProc("DoNothing1"));
        assertTrue(execUserProc("DoNothing2"));
        assertTrue(execUserProc("DoNothing3"));

        //close connection
        CloseUserConnection();
    }

    @Test
    public void testUserWithDefaultProcPerm() throws Exception{
        // userWithDefaultProcPerm can run all default procedures
        Properties props = new Properties();
        props.setProperty("user", "userWithDefaultProcPerm");
        props.setProperty("password", "password");
        myconn = getJdbcConnection("jdbc:voltdb://localhost:21212", props);

        //sysproc should fail
        assertFalse(execSysProc());

        // adhoc should fail
        assertFalse(execAdhocProc());
        assertFalse(execAdhocReadProc());

        //default proc should pass
        assertTrue(execDefaultProc());
        assertTrue(execDefaultReadProc());

        //user proc DoNothing1 should fail
        //user proc DoNothing2 should fail
        //user proc DoNothing3 should fail
        assertFalse(execUserProc("DoNothing1"));
        assertFalse(execUserProc("DoNothing2"));
        assertFalse(execUserProc("DoNothing3"));

        //close connection
        CloseUserConnection();
    }


    @Test
    public void testUserWithDefaultProcReadPerm() throws Exception{
        // userWithDefaultProcReadPerm can run read only default procedures
        Properties props = new Properties();
        props.setProperty("user", "userWithDefaultProcReadPerm");
        props.setProperty("password", "password");
        myconn = getJdbcConnection("jdbc:voltdb://localhost:21212", props);

        //sysproc should fail
        assertFalse(execSysProc());

        // adhoc should fail
        assertFalse(execAdhocProc());
        assertFalse(execAdhocReadProc());

        //default Write proc should fail
        //default Read proc should pass
        assertFalse(execDefaultProc());
        assertTrue(execDefaultReadProc());

        //user proc DoNothing1 should fail
        //user proc DoNothing2 should fail
        //user proc DoNothing3 should fail
        assertFalse(execUserProc("DoNothing1"));
        assertFalse(execUserProc("DoNothing2"));
        assertFalse(execUserProc("DoNothing3"));

        //close connection
        CloseUserConnection();
    }

    @Test
    public void testUserWithAllProcPerm() throws Exception{
        // userWithAllProcPerm can run user defined procedures
        Properties props = new Properties();
        props.setProperty("user", "userWithAllProcPerm");
        props.setProperty("password", "password");
        myconn = getJdbcConnection("jdbc:voltdb://localhost:21212", props);

        //sysproc should fail
        assertFalse(execSysProc());

        // adhoc should fail
        assertFalse(execAdhocProc());
        assertFalse(execAdhocReadProc());

        //default Write proc should fail
        //default Read proc should fail
        assertFalse(execDefaultProc());
        assertFalse(execDefaultReadProc());

        //user proc DoNothing1 should pass
        //user proc DoNothing2 should pass
        //user proc DoNothing3 should pass
        assertTrue(execUserProc("DoNothing1"));
        assertTrue(execUserProc("DoNothing2"));
        assertTrue(execUserProc("DoNothing3"));

        //close connection
        CloseUserConnection();
    }

    @Test
    public void testUserWithoutDefaultProcPerm() throws Exception{
        // userWithoutDefaultProcPerm can run no procedures
        Properties props = new Properties();
        props.setProperty("user", "userWithoutDefaultProcPerm");
        props.setProperty("password", "password");
        myconn = getJdbcConnection("jdbc:voltdb://localhost:21212", props);

        //sysproc should fail
        assertFalse(execSysProc());

        // adhoc should fail
        assertFalse(execAdhocProc());
        assertFalse(execAdhocReadProc());

        //default Write proc should fail
        //default Read proc should fail
        assertFalse(execDefaultProc());
        assertFalse(execDefaultReadProc());

        //user proc DoNothing1 should fail
        //user proc DoNothing2 should fail
        //user proc DoNothing3 should fail
        assertFalse(execUserProc("DoNothing1"));
        assertFalse(execUserProc("DoNothing2"));
        assertFalse(execUserProc("DoNothing3"));

        //close connection
        CloseUserConnection();
    }

    private boolean execAdhocProc() throws SQLException{
        CallableStatement stmt = myconn.prepareCall("{call @AdHoc(?) }");
        stmt.setString(1, "UPSERT INTO T VALUES(2,3.0,4.0);");
        try {
            stmt.execute();
        } catch (SQLException e) {
            return false;
        }
        return true;
    }

    private boolean execAdhocReadProc() throws SQLException{
        CallableStatement stmt = myconn.prepareCall("{call @AdHoc(?) }");
        stmt.setString(1, "SELECT COUNT(*) FROM T;");
        try {
            stmt.execute();
        } catch (SQLException e) {
            return false;
        }
        return true;
    }


    private boolean execUserProc(String userProc)  throws SQLException{
        CallableStatement stmt = myconn.prepareCall("{call " + userProc + "(?) }");
        stmt.setLong(1, 2);
        try {
            stmt.execute();
        } catch (SQLException e) {
            return false;
        }
        return true;
    }


    private boolean execDefaultProc()  throws SQLException{
        CallableStatement stmt = myconn.prepareCall("{call T.upsert(?,?,?) }");
        stmt.setInt(1, 1);
        stmt.setDouble(2, 3.0);
        stmt.setDouble(3, 4.0);
        try {
            stmt.execute();
        } catch (SQLException e) {
            return false;
        }
        return true;
    }

    private boolean execDefaultReadProc()  throws SQLException{
        CallableStatement stmt = myconn.prepareCall("{call T.select (?) }");
        stmt.setInt(1, 1);
        try {
            stmt.execute();
        } catch (SQLException e) {
            return false;
        }
        return true;
    }

    private boolean execSysProc() throws SQLException{
        CallableStatement stmt = myconn.prepareCall("{call @Quiesce }");
        try {
            stmt.execute();
        } catch (SQLException e) {
            return false;
        }
        return true;
    }
}
