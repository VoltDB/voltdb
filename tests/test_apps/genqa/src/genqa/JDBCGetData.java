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

package genqa;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import genqa.VerifierUtils.Config;

public class JDBCGetData {
    static Connection conn;
    static String selectSql = "SELECT * FROM \"EXPORT_PARTITIONED_TABLE\" where \"ROWID\" = ?;";
    static PreparedStatement selectStmt = null;

    /**
     * Connect and prepare the select to make the per row handling
     * as efficient as possible, though it's still pretty pokey
     * @param config
     * @return
     */
    public static Connection jdbcConnect(Config config) {
        try {
            Class.forName(config.driver);
        } catch (ClassNotFoundException e) {
            System.err.println("Could not find the JDBC driver class.\n");
            e.printStackTrace();
            System.exit(-1);
        }

        String connectString = "jdbc:" + config.jdbcDBMS + "://" + config.host_port + "/" + config.jdbcDatabase;
        try {
            conn = DriverManager.getConnection(connectString, config.jdbcUser, config.jdbcPassword);
            selectStmt = conn.prepareStatement(selectSql);
        } catch (SQLException e) {
            System.err.println("Could not connect to the database with connect string " + connectString + ".\n");
            e.printStackTrace();
            System.exit(-1);
        }
        return conn;
    }

    static ResultSet jdbcRead(long rowid) {
        ResultSet rs = null;
        try {
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
