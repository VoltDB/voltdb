/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb;

import java.sql.Connection;
import java.sql.Statement;

import org.voltcore.logging.VoltLogger;
import org.voltdb.utils.CompressionService;
import org.voltdb.utils.Encoder;

/**
 * A wrapper around the HSQLDB engine. This class can be used to execute SQL
 * statements instead of the C++ ExecutionEngine. It is currently used only
 * by the SQL Coverage and JUnit regressionsuite tests.
 */
public class HsqlBackend extends NonVoltDBBackend {
    /** java.util.logging logger. */
    @SuppressWarnings("unused")
    private static final VoltLogger log = new VoltLogger(HsqlBackend.class.getName());

    static public HsqlBackend initializeHSQLBackend(long siteId, CatalogContext context)
    {
        synchronized(backendLock) {
            if (m_backend == null) {
                try {
                    m_backend = new HsqlBackend(siteId);
                    final String binDDL = context.database.getSchema();
                    final String ddl = CompressionService.decodeBase64AndDecompress(binDDL);
                    final String[] commands = ddl.split("\n");
                    for (String command : commands) {
                        String decoded_cmd = Encoder.hexDecodeToString(command);
                        decoded_cmd = decoded_cmd.trim();
                        if (decoded_cmd.length() == 0) {
                            continue;
                        }
                        m_backend.runDDL(decoded_cmd);
                    }
                }
                catch (final Exception ex) {
                    hostLog.fatal("Unable to construct HSQL backend");
                    VoltDB.crashLocalVoltDB(ex.getMessage(), true, ex);
                }
            }
            return (HsqlBackend) m_backend;
        }
    }

    /** Constructor specifying a siteId, which is used in the connectionURL. */
    public HsqlBackend(long siteId) {
        super("HSQL", "org.hsqldb_voltpatches.jdbcDriver",
              "jdbc:hsqldb:mem:x" + String.valueOf(siteId), "sa", "");
    }

    /** Creates a new HsqlBackend wrapping dbconn. This is (was?) used for testing only. */
    private HsqlBackend(Connection dbconn) {
        super(dbconn);
    }

    @Override
    protected void shutdown() {
        try {
            try {
                Statement stmt = dbconn.createStatement();
                stmt.execute("SHUTDOWN;");
            } catch (Exception e) {};
            dbconn.close();
            dbconn = null;
            System.gc();
        } catch (Exception e) {
            hostLog.error("Error shutting down backend", e);
        }
    }

}
