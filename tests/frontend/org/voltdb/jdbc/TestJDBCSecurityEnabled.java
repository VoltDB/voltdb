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

package org.voltdb.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
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
        new RoleInfo("GroupWithSQLPerm", true, false, false, false, false, false),
        new RoleInfo("GroupWithSQLReadPerm", false, true, false, false, false, false),
        new RoleInfo("GroupWithAdminPerm", false, false, true, false, false, false),
        new RoleInfo("GroupWithDefaultProcPerm", false, false, false, true, false, false),
        new RoleInfo("GroupWithDefaultProcReadPerm", false, false, false, false, true, false),
        new RoleInfo("GroupWithAllProcPerm", false, false, false, false, false, true),
        new RoleInfo("GroupWithNoPerm", false, false, false, false, false, false),
        new RoleInfo("GroupWithNoPerm2", false, false, false, false, false, false) // group with same permission, used for test user procedures
    };

    public static final UserInfo[] USERS = new UserInfo[] {
        new UserInfo("userWithSQLPerm", "password", new String[] {"GroupWithSQLPerm"}),
        new UserInfo("userWithSQLReadPerm", "password", new String[] {"GroupWithSQLReadPerm"}),
        new UserInfo("userWithAdminPerm", "password", new String[] {"GroupWithAdminPerm"}),
        new UserInfo("userWithDefaultProcPerm", "password", new String[] {"GroupWithDefaultProcPerm"}),
        new UserInfo("userWithDefaultProcReadPerm", "password", new String[] {"GroupWithDefaultProcReadPerm"}),
        new UserInfo("userWithAllProcPerm", "password", new String[] {"GroupWithAllProcPerm"}),
        new UserInfo("userWithNoPerm", "password", new String[] {"GroupWithNoPerm"}),
        new UserInfo("userWithNoPerm2", "password", new String[] {"GroupWithNoPerm2"}),
        new UserInfo("userWithDefaultUserPerm", "password", new String[] {"User"}), // user of default User Group
        new UserInfo("userWithDefaultAdminPerm", "password", new String[] {"ADMINISTRATOR"}) // user of default "ADMINISTRATOR" Group
    };

    public static final ProcedureInfo[] PROCEDURES = {
        new ProcedureInfo(new String[0], DoNothing1.class),
        new ProcedureInfo(new String[] { "GroupWithNoPerm" }, DoNothing2.class),
        new ProcedureInfo(new String[] { "GroupWithNoPerm", "GroupWithNoPerm2" }, DoNothing3.class)
    };

    static final Map<String,Boolean[]> ExpectedResultMap  = new HashMap<String,Boolean[]>() {{
        // tests procedures for each user in the order of
        // adhoc proc, adhoc read proc, admin proc, default proc, defaultread proc
        // user procs DoNothin1, DoNothing2, DoNothing3
        put("userWithSQLPerm", new Boolean[] {true,true,false,true,true,false,false,false});
        put("userWithSQLReadPerm", new Boolean[] {false,true,false,false,true,false,false,false});
        put("userWithAdminPerm", new Boolean[] {true,true,true,true,true,true,true,true});
        put("userWithDefaultProcPerm", new Boolean[] {false,false,false,true,true,false,false,false});
        put("userWithDefaultProcReadPerm", new Boolean[] {false,false,false,false,true,false,false,false});
        put("userWithAllProcPerm", new Boolean[] {false,false,false,false,false,true,true,true});
        put("userWithNoPerm", new Boolean[] {false,false,false,false,false,false,true,true});
        put("userWithNoPerm2", new Boolean[] {false,false,false,false,false,false,false,true});
        put("userWithDefaultUserPerm", new Boolean[] {true,true,false,true,true,true,true,true});
        put("userWithDefaultAdminPerm", new Boolean[] {true,true,true,true,true,true,true,true});
    }};

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
        props.setProperty("user", "userWithAdminPerm");
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
        props.setProperty("user", "userWithAdminPerm");
        props.setProperty("password", "password");
        try {
            myconn = getJdbcConnection("jdbc:voltdb://localhost:21212", props);
            CloseUserConnection();
        }catch (Exception e) {
            e.printStackTrace();
            fail("Connection creation shouldn't fail: " + e.getMessage());
        }
    }

    @Test
    public void testPerms() throws Exception {
        Properties props = new Properties();
        props.setProperty("password", "password");

        for (Entry<String, Boolean[]> entry : ExpectedResultMap.entrySet()) {
            String userName = entry.getKey();
            Boolean[] expectedRet = entry.getValue();
            props.setProperty("user", userName);
            myconn = getJdbcConnection("jdbc:voltdb://localhost:21212", props);
            assertEquals(userName + " has wrong perms", expectedRet, processProc());
            //close connection
            CloseUserConnection();
        }
    }

    private Boolean[] processProc() throws SQLException {
        return new Boolean[] {execAdhocProc(),execAdhocReadProc(),execSysProc(),execDefaultProc(),execDefaultReadProc(),
                execUserProc("DoNothing1"),execUserProc("DoNothing2"),execUserProc("DoNothing3")};
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
