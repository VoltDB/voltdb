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

#include "storage/TableDiskHeader.h"

#include "harness.h"
#include "catalog/catalog.h"
#include "catalog/catalogmap.h"
#include "catalog/cluster.h"
#include "catalog/database.h"
#include "catalog/host.h"
#include "catalog/partition.h"
#include "catalog/site.h"
#include "catalog/table.h"

#include "boost/shared_ptr.hpp"
#include <sstream>

using namespace voltdb;
using namespace std;

namespace
{
    static const string TABLE_NAME = "test_table";
    static const string REPL_TABLE_NAME = "test_repl_table";
    static const string DATABASE_NAME = "database";
    static const string CLUSTER_NAME = "cluster";
    static const string SITE_NAME = "1";
    static const string HOST_NAME = "0";
    static const string PARTITION_NAME_1 = "0";
    static const string PARTITION_NAME_2 = "1";
    static const string PARTITION_NAME_3 = "2";

    int string_to_int(const string& aString)
    {
        return atoi(aString.c_str());
    }

    string int_to_string(int anInt)
    {
        stringstream temp_stream;
        temp_stream << anInt;
        return temp_stream.str();
    }
}

class TableDiskHeaderTest : public Test
{
public:
    TableDiskHeaderTest()
    {
    }

    ~TableDiskHeaderTest()
    {
    }
};

TEST_F(TableDiskHeaderTest, RoundTrip)
{
    // Construct easy catalog stuff
    string cat_statement;
    cat_statement.append("add / clusters ").append(CLUSTER_NAME);

    string cluster_path;
    cluster_path.append("/clusters[").append(CLUSTER_NAME).append("]");

    cat_statement.append("\nadd ").append(cluster_path).
        append(" databases ").append(DATABASE_NAME);

    string database_path;
    database_path.append("/databases[").append(DATABASE_NAME).append("]");

    cat_statement.append("\nadd ").append(cluster_path).append(database_path).
        append(" tables ").append(TABLE_NAME);

    string table_path;
    table_path.append("/tables[").append(TABLE_NAME).append("]");

    cat_statement.append("\nset ").append(cluster_path).append(database_path).
        append(table_path).append(" isreplicated false");

    cat_statement.append("\nadd ").append(cluster_path).append(database_path).
        append(" tables ").append(REPL_TABLE_NAME);

    string repl_table_path;
    repl_table_path.append("/tables[").append(REPL_TABLE_NAME).append("]");

    cat_statement.append("\nset ").append(cluster_path).append(database_path).
        append(repl_table_path).append(" isreplicated true");

    cat_statement.append("\nadd ").append(cluster_path).append(" hosts ").
        append(HOST_NAME);
    cat_statement.append("\nadd ").append(cluster_path).append(" partitions ").
        append(PARTITION_NAME_1);
    cat_statement.append("\nadd ").append(cluster_path).append(" partitions ").
        append(PARTITION_NAME_2);
    cat_statement.append("\nadd ").append(cluster_path).append(" partitions ").
        append(PARTITION_NAME_3);
    cat_statement.append("\nadd ").append(cluster_path).append(" sites ").
        append(SITE_NAME);
    string site_path;
    site_path.append("/sites[").append(SITE_NAME).append("]");
    cat_statement.append("\nset ").append(cluster_path).append(site_path).
        append(" isexec true");

    cat_statement.append("\nset ").append(cluster_path).append(site_path).
        append(" host ").append(cluster_path).append("/hosts[").append(HOST_NAME).
        append("]");
    cat_statement.append("\nset ").append(cluster_path).append(site_path).
        append(" partition ").append(cluster_path).append("/partitions[").append(PARTITION_NAME_1).
        append("]");

    catalog::Catalog test_catalog;
    test_catalog.execute(cat_statement);

    // now get the values that depend on walking the catalog
    cat_statement.clear();
    catalog::CatalogMap<catalog::Cluster> clusters = test_catalog.clusters();
    catalog::Cluster* cluster = clusters.get(CLUSTER_NAME);
    assert(cluster);
    catalog::CatalogMap<catalog::Host> hosts = cluster->hosts();
    catalog::Host* host = hosts.get(HOST_NAME);
    assert(host);
    catalog::CatalogMap<catalog::Partition> partitions = cluster->partitions();

    catalog::CatalogMap<catalog::Database> databases = cluster->databases();
    catalog::Database* database = databases.get(DATABASE_NAME);
    assert(database);
    catalog::CatalogMap<catalog::Table> tables = database->tables();
    catalog::Table* table = tables.get(TABLE_NAME);
    assert(table);
    catalog::Table* repl_table = tables.get(REPL_TABLE_NAME);
    assert(repl_table);
    catalog::CatalogMap<catalog::Site> sites = cluster->sites();
    catalog::Site* site = sites.get(SITE_NAME);
    assert(site);

    // do the replicated table
    stringstream repl_test_buf(stringstream::in | stringstream::out |
                               stringstream::binary);

    boost::shared_ptr<TableDiskHeader> header =
        TableDiskHeader::constructHeaderOnSave(test_catalog,
                                               repl_table->relativeIndex(),
                                               string_to_int(SITE_NAME));
    EXPECT_EQ(header->getTableName(), REPL_TABLE_NAME);
    EXPECT_EQ(header->getDatabaseName(), DATABASE_NAME);
    EXPECT_EQ(header->getClusterName(), CLUSTER_NAME);
    EXPECT_EQ(header->getSiteId(), string_to_int(SITE_NAME));
    EXPECT_EQ(header->getHostId(), string_to_int(HOST_NAME));
    EXPECT_EQ(header->isReplicated(), true);

    header->writeHeader(repl_test_buf);
    boost::shared_ptr<TableDiskHeader> header2 =
        TableDiskHeader::constructHeaderOnRestore(repl_test_buf);
    EXPECT_EQ(header2->getTableName(), REPL_TABLE_NAME);
    EXPECT_EQ(header2->getDatabaseName(), DATABASE_NAME);
    EXPECT_EQ(header2->getClusterName(), CLUSTER_NAME);
    EXPECT_EQ(header2->getSiteId(), string_to_int(SITE_NAME));
    EXPECT_EQ(header2->getHostId(), string_to_int(HOST_NAME));
    EXPECT_EQ(header2->isReplicated(), true);

    // do the non-replicated table
    stringstream test_buf(stringstream::in | stringstream::out |
                          stringstream::binary);

    header = TableDiskHeader::constructHeaderOnSave(test_catalog,
                                                    table->relativeIndex(),
                                                    string_to_int(SITE_NAME));
    EXPECT_EQ(header->getTableName(), TABLE_NAME);
    EXPECT_EQ(header->getDatabaseName(), DATABASE_NAME);
    EXPECT_EQ(header->getClusterName(), CLUSTER_NAME);
    EXPECT_EQ(header->getSiteId(), string_to_int(SITE_NAME));
    EXPECT_EQ(header->getHostId(), string_to_int(HOST_NAME));
    EXPECT_EQ(header->isReplicated(), false);
    EXPECT_EQ(header->getPartitionId(), string_to_int(PARTITION_NAME_1));
    EXPECT_EQ(header->getTotalPartitions(), 3);

    header->writeHeader(test_buf);
    header2 = TableDiskHeader::constructHeaderOnRestore(test_buf);
    EXPECT_EQ(header2->getTableName(), TABLE_NAME);
    EXPECT_EQ(header2->getDatabaseName(), DATABASE_NAME);
    EXPECT_EQ(header2->getClusterName(), CLUSTER_NAME);
    EXPECT_EQ(header2->getSiteId(), string_to_int(SITE_NAME));
    EXPECT_EQ(header2->getHostId(), string_to_int(HOST_NAME));
    EXPECT_EQ(header2->isReplicated(), false);
    EXPECT_EQ(header2->getPartitionId(), string_to_int(PARTITION_NAME_1));
    EXPECT_EQ(header2->getTotalPartitions(), 3);
}

int main()
{
    return TestSuite::globalInstance()->runAll();
}
