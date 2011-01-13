/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.client;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Encapsulates configuration settings for client statistics loading. This is not production
 * ready and the schema is subject to change and there are no tools provided to initialize a database
 * with the schema nor is the schema documented.
 *
 */
public class StatsUploaderSettings {
    final String databaseURL;
    final String applicationName;
    final String subApplicationName;
    final int pollInterval;
    final Connection conn;

    /**
     *
     * Constructor that stores settings and verifies that a connection to the specified
     * databaseURL can be created
     * @param databaseURL URL of the database to connect to. Must include login credentials in URL.
     * For example "jdbc:mysql://[host][,failoverhost...][:port]/[database][?propertyName1][=propertyValue1][&propertyName2][=propertyValue2]..."
     * @param applicationName Identifies this application in the statistics tables
     * @param subApplicationName More specific aspect of this application (Client, Loader etc.)
     * @param pollInterval Interval in milliseconds that stats should be polled and uploaded
     * @throws SQLException Thrown if a connection to the database can't be created or if the appropriate JDBC
     * driver can't be found.
     */
    public StatsUploaderSettings(
            String databaseURL,
            String applicationName,
            String subApplicationName,
            int pollInterval) {
        this.databaseURL = databaseURL;
        this.applicationName = applicationName;
        this.subApplicationName = subApplicationName;
        this.pollInterval = pollInterval;

        if (applicationName == null || applicationName.isEmpty()) {
            throw new IllegalArgumentException("Application name is null or empty");
        }

        if (pollInterval < 1000) {
            throw new IllegalArgumentException("Polling more then once per second is excessive");
        }

        if ((databaseURL == null) || (databaseURL.isEmpty())) {
            String msg = "Not connecting to SQL reporting server as connection URL is null or missing.";
            throw new RuntimeException(msg);
        }

        try {
            conn = DriverManager.getConnection(databaseURL);
            conn.setAutoCommit(false);
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        }
        catch (Exception e) {
            String msg = "Failed to connect to SQL reporting server with message:\n    ";
            msg += e.getMessage();
            throw new RuntimeException(msg);
        }
    }
}