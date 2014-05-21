/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

package org.voltdb;


import java.io.IOException;

import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;

import junit.framework.TestCase;

public class AdhocDDLTestBase extends TestCase {

    //adHocQuery = "CREATE TABLE ICAST2 (C1 INT, C2 FLOAT);";
    //adHocQuery = "CREATE INDEX IDX_PROJ_PNAME ON PROJ(PNAME);";
    //adHocQuery = "DROP TABLE PROJ;";
    //adHocQuery = "PARTITION TABLE PROJ ON COLUMN PNUM;";
    //adHocQuery = "CREATE PROCEDURE AS SELECT 1 FROM PROJ;";
    //adHocQuery = "CREATE PROCEDURE FROM CLASS bar.Foo;";

    protected ServerThread m_localServer;
    protected Client m_client;

    protected void startSystem(VoltDB.Configuration config) throws Exception
    {
        m_localServer = new ServerThread(config);
        m_localServer.start();
        m_localServer.waitForInitialization();
        m_client = ClientFactory.createClient();
        m_client.createConnection("localhost");
    }

    protected void teardownSystem() throws Exception
    {
        if (m_client != null) { m_client.close(); }
        m_client = null;

        if (m_localServer != null) {
            m_localServer.shutdown();
            m_localServer.join();
        }
        m_localServer = null;
    }


    protected boolean findTableInSystemCatalogResults(String table) throws Exception
    {
        VoltTable tables = m_client.callProcedure("@SystemCatalog", "TABLES").getResults()[0];
        boolean found = false;
        tables.resetRowPosition();
        while (tables.advanceRow()) {
            String thisTable = tables.getString("TABLE_NAME");
            if (thisTable.equalsIgnoreCase(table)) {
                found = true;
                break;
            }
        }
        return found;
    }

    protected boolean findIndexInSystemCatalogResults(String index) throws Exception
    {
        VoltTable indexinfo = m_client.callProcedure("@SystemCatalog", "INDEXINFO").getResults()[0];
        boolean found = false;
        indexinfo.resetRowPosition();
        while (indexinfo.advanceRow()) {
            String thisindex = indexinfo.getString("INDEX_NAME");
            if (thisindex.equalsIgnoreCase(index)) {
                found = true;
                break;
            }
        }
        return found;
    }

    protected boolean verifyTableColumnType(String table, String column, String type)
        throws Exception
    {
        VoltTable columns = m_client.callProcedure("@SystemCatalog", "COLUMNS").getResults()[0];
        boolean verified = false;
        columns.resetRowPosition();
        while (columns.advanceRow()) {
            String thiscolumn = columns.getString("COLUMN_NAME");
            String thistable = columns.getString("TABLE_NAME");
            String thistype = columns.getString("TYPE_NAME");
            if (thistable.equalsIgnoreCase(table) && thiscolumn.equalsIgnoreCase(column) &&
                thistype.equalsIgnoreCase(type)) {
                verified = true;
                break;
            }
        }
        return verified;
    }

    protected boolean verifyTableColumnSize(String table, String column, int size)
        throws Exception
    {
        VoltTable columns = m_client.callProcedure("@SystemCatalog", "COLUMNS").getResults()[0];
        boolean verified = false;
        columns.resetRowPosition();
        while (columns.advanceRow()) {
            String thiscolumn = columns.getString("COLUMN_NAME");
            String thistable = columns.getString("TABLE_NAME");
            int thissize = (int)columns.getLong("COLUMN_SIZE");
            if (thistable.equalsIgnoreCase(table) && thiscolumn.equalsIgnoreCase(column) &&
                thissize == size) {
                verified = true;
                break;
            }
        }
        return verified;
    }
}
