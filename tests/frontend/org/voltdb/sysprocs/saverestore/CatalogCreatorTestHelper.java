/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb.sysprocs.saverestore;

import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;

/*
 * Convenience class for tests that need a simple catalog.
 *
 * Basically, just front a bunch of the repetitive catalog walking with
 * more convenient methods.
 */
public class CatalogCreatorTestHelper
{
    public CatalogCreatorTestHelper(String clusterName, String databaseName)
    {
        m_clusterName = clusterName;
        m_databaseName = databaseName;
        m_catalog = new Catalog();
        m_catalog.execute("add / clusters " + m_clusterName);
        m_catalog.execute("add " + getCluster().getPath() + " databases " +
                          m_databaseName);
    }

    Catalog addTable(String tableName, boolean isReplicated)
    {
        m_catalog.execute("add " + getDatabase().getPath() + " tables " +
                          tableName);
        String is_replicated = "false";
        if (isReplicated)
        {
            is_replicated = "true";
        }
        m_catalog.execute("set " + getTable(tableName).getPath() +
                          " isreplicated " + is_replicated);
        return m_catalog;
    }

    Catalog addColumnToTable(String tableName, String columnName,
                             VoltType columnType,
                             boolean isNullable, String defaultValue,
                             VoltType defaultType)
    {
        int index = getTable(tableName).getColumns().size();
        String table_path = getTable(tableName).getPath();
        m_catalog.execute("add " + table_path + " columns " + columnName);
        String column_path =
            getColumnFromTable(tableName, columnName).getPath();
        m_catalog.execute("set " + column_path + " index " + index);
        m_catalog.execute("set " + column_path + " type " +
                          columnType.getValue());
        m_catalog.execute("set " + column_path + " nullable " +
                          String.valueOf(isNullable));
        m_catalog.execute("set " + column_path + " name \"" + columnName +
                          "\"");
        m_catalog.execute("set " + column_path + " defaultvalue \"" +
                          defaultValue + "\"");
        m_catalog.execute("set " + column_path + " defaulttype " +
                          defaultType.getValue());
        return m_catalog;
    }

    public Catalog addHost(int hostId)
    {
        m_catalog.execute("add " + getCluster().getPath() + " hosts " + hostId);
        return m_catalog;
    }

    public Catalog addPartition(int partitionId)
    {
        m_catalog.execute("add " + getCluster().getPath() + " partitions " +
                          partitionId);
        return m_catalog;
    }

    public Catalog addSite(int siteId, int hostId, int partitionId, boolean isExec)
    {
        m_catalog.execute("add " + getCluster().getPath() + " sites " + siteId);
        m_catalog.execute("set " + getSite(siteId).getPath() + " host " +
                          getHost(hostId).getPath());
        m_catalog.execute("set " + getSite(siteId).getPath() + " isexec " +
                          isExec);
        String partition_path = "null";
        if (isExec)
        {
            partition_path = getPartition(partitionId).getPath();
        }
        m_catalog.execute("set " + getSite(siteId).getPath() + " partition " +
                partition_path);
        return m_catalog;
    }

    void dumpCommands()
    {
        System.out.println(m_catalog.serialize());
    }

    public Catalog getCatalog()
    {
        return m_catalog;
    }

    Cluster getCluster()
    {
        return m_catalog.getClusters().get(m_clusterName);
    }

    Database getDatabase()
    {
        return getCluster().getDatabases().get(m_databaseName);
    }

    Table getTable(String tableName)
    {
        return getDatabase().getTables().get(tableName);
    }

    Column getColumnFromTable(String tableName, String columnName)
    {
        return getTable(tableName).getColumns().get(columnName);
    }

    Host getHost(int hostId)
    {
        return getCluster().getHosts().get(String.valueOf(hostId));
    }

    Partition getPartition(int partitionId)
    {
        return getCluster().getPartitions().get(String.valueOf(partitionId));
    }

    Site getSite(int siteId)
    {
        return getCluster().getSites().get(String.valueOf(siteId));
    }

    String m_clusterName;
    String m_databaseName;
    Catalog m_catalog;
}
