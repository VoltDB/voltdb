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

package genqa;

import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import genqa.VerifierUtils.Config;

public class JDBCGetData {
    static Connection conn;
    static String selectSql = "SELECT * FROM \"EXPORT_PARTITIONED_TABLE_JDBC\" where \"ROWID\" = ?;";
    static String selectGeoSql = "SELECT * FROM \"EXPORT_GEO_PARTITIONED_TABLE_JDBC\" where \"ROWID\" = ?;";
    static PreparedStatement selectStmt = null;
    private static Config config;
    /**
     * Connect and prepare the select to make the per row handling
     * as efficient as possible, though it's still pretty pokey
     * @param config
     * @return
     */
    public static Connection jdbcConnect(Config lconfig) {
        config = lconfig;
        /* some jdbc drivers may not support connection timout semantics,
           so pre-test the connection.
         */
        String[] a = config.host_port.split(":");
        try {
            Socket s = new Socket(a[0], Integer.valueOf(a[1]));
            s.close();
        } catch (Exception e) {
            System.err.println("Service is not up at '" + config.host_port + "' " + e);
            e.printStackTrace();
            System.exit(-1);
        }

        try {
            Class.forName(config.driver);
        } catch (ClassNotFoundException e) {
            System.err.println("Could not find the JDBC driver class.\n");
            e.printStackTrace();
            System.exit(-1);
        }

        String connectString = "jdbc:" + config.jdbcDBMS + "://" + config.host_port + "/" + config.jdbcDatabase;
        Properties myProp = new Properties();
        myProp.put("user", config.jdbcUser);
        myProp.put("password", config.jdbcPassword);
        try {
            conn = DriverManager.getConnection(connectString, myProp);
            System.out.println("Connected!");
        } catch (SQLException e) {
            System.err.println("Could not connect to the database with connect string " + connectString + ", exception: " + e);
            e.printStackTrace();
            System.exit(-1);
        }
        return conn;
    }

    static ResultSet jdbcRead(long rowid) {
        ResultSet rs = null;
        try {
            if ( selectStmt == null ) {
                if ( config.usegeo ) {
                    selectStmt = conn.prepareStatement(selectGeoSql);
                } else {
                    selectStmt = conn.prepareStatement(selectSql);
                }
            }
            selectStmt.setLong(1, rowid);
            rs = selectStmt.executeQuery();
        } catch (SQLException e) {
            System.err.println("Exception in DB row access.\n");
            e.printStackTrace();
            System.exit(-1);
        }
        return rs;
    }
}
