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


#include "harness.h"
#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "catalog/database.h"
#include "catalog/table.h"
#include "common/common.h"
#include "execution/VoltDBEngine.h"
#include "storage/table.h"

#include <cstdlib>

using namespace voltdb;
using namespace std;

class AddDropTableTest : public Test {

  public:
    AddDropTableTest()
        : m_clusterId(0), m_databaseId(0), m_siteId(0), m_partitionId(0),
          m_hostId(101), m_hostName("host101")
    {
        m_engine = new VoltDBEngine();

        m_resultBuffer = new char[1024 * 1024 * 2];
        m_exceptionBuffer = new char[4096];
        m_engine->setBuffers(NULL, 0,
                             m_resultBuffer, 1024 * 1024 * 2,
                             m_exceptionBuffer, 4096);

        m_engine->resetReusedResultOutputBuffer();
        int partitionCount = 3;
        m_engine->initialize(m_clusterId,
                             m_siteId,
                             m_partitionId,
                             m_hostId,
                             m_hostName,
                             DEFAULT_TEMP_TABLE_MEMORY);
        m_engine->updateHashinator( HASHINATOR_LEGACY,
                                   (char*)&partitionCount,
                                    NULL,
                                    0);

        std::string initialCatalog =
          "add / clusters cluster\n"
          "add /clusters#cluster databases database\n"
          "add /clusters#cluster/databases#database programs program\n";

        bool loadResult = m_engine->loadCatalog( -2, initialCatalog);
        ASSERT_TRUE(loadResult);
    }

    ~AddDropTableTest()
    {
        delete m_engine;
        delete[] m_resultBuffer;
        delete[] m_exceptionBuffer;
    }


    std::string tableACmds()
    {
        return
          "add /clusters#cluster/databases#database tables tableA\n"
          "set /clusters#cluster/databases#database/tables#tableA type 0\n"
          "set /clusters#cluster/databases#database/tables#tableA isreplicated false\n"
          "set /clusters#cluster/databases#database/tables#tableA partitioncolumn 0\n"
          "set /clusters#cluster/databases#database/tables#tableA estimatedtuplecount 0\n"
          "add /clusters#cluster/databases#database/tables#tableA columns A\n"
          "set /clusters#cluster/databases#database/tables#tableA/columns#A index 0\n"
          "set /clusters#cluster/databases#database/tables#tableA/columns#A type 5\n"
          "set /clusters#cluster/databases#database/tables#tableA/columns#A size 0\n"
          "set /clusters#cluster/databases#database/tables#tableA/columns#A nullable false\n"
          "set /clusters#cluster/databases#database/tables#tableA/columns#A name \"A\"";
    }

    std::string tableADeleteCmd()
    {
        return "delete /clusters#cluster/databases#database tables tableA";
    }

    std::string tableBCmds()
    {
        return
          "add /clusters#cluster/databases#database tables tableB\n"
          "set /clusters#cluster/databases#database/tables#tableB type 0\n"
          "set /clusters#cluster/databases#database/tables#tableB isreplicated false\n"
          "set /clusters#cluster/databases#database/tables#tableB partitioncolumn 0\n"
          "set /clusters#cluster/databases#database/tables#tableB estimatedtuplecount 0\n"
          "add /clusters#cluster/databases#database/tables#tableB columns A\n"
          "set /clusters#cluster/databases#database/tables#tableB/columns#A index 0\n"
          "set /clusters#cluster/databases#database/tables#tableB/columns#A type 5\n"
          "set /clusters#cluster/databases#database/tables#tableB/columns#A size 0\n"
          "set /clusters#cluster/databases#database/tables#tableB/columns#A nullable false\n"
          "set /clusters#cluster/databases#database/tables#tableB/columns#A name \"A\"";
    }

    std::string tableBDeleteCmd()
    {
        return "delete /clusters#cluster/databases#database tables tableB";
    }


  protected:
    CatalogId m_clusterId;
    CatalogId m_databaseId;
    CatalogId m_siteId;
    CatalogId m_partitionId;
    CatalogId m_hostId;
    std::string m_hostName;
    VoltDBEngine *m_engine;
    char *m_resultBuffer;
    char *m_exceptionBuffer;
};

/*
 * Test on catalog.
 * Verify new table has the add flag set.
 */
TEST_F(AddDropTableTest, DetectNewTable)
{
    // add a table to voltdbengine's catalog
    catalog::Catalog *catalog = m_engine->getCatalog();
    catalog->execute(tableACmds());

    bool found = false;
    catalog::Cluster *cluster = catalog->clusters().get("cluster");
    catalog::Database *db = cluster->databases().get("database");

    // get the table and see that is newly added
    // also assert that it really exists in the new catalog.
    map<string, catalog::Table*>::const_iterator t_iter;
    for (t_iter = db->tables().begin();
         t_iter != db->tables().end();
         t_iter++)
    {
        catalog::Table *t = t_iter->second;
        if (t->name() == "tableA") {
            ASSERT_TRUE(t->wasAdded());
            found = true;
        }
        else {
            ASSERT_FALSE(t->wasAdded());
        }
    }
    ASSERT_TRUE(found);
}

/*
 * Test on catalog.
 * Delete a table and make sure it is absent.
 */
TEST_F(AddDropTableTest, DetectDeletedTable)
{
    catalog::Catalog *catalog = m_engine->getCatalog();
    catalog->execute(tableACmds());

    bool found = false;
    catalog::Cluster *cluster = catalog->clusters().get("cluster");
    catalog::Database *db = cluster->databases().get("database");

    // delete the table and verify its absence
    catalog->execute(tableADeleteCmd());
    map<string, catalog::Table*>::const_iterator t_iter;
    for (found = false, t_iter = db->tables().begin();
         t_iter != db->tables().end();
         t_iter++)
    {
        catalog::Table *t = t_iter->second;
        if (t->name() == "tableA") {
            found = true;
        }
        else {
            ASSERT_FALSE(t->wasAdded());
        }
    }
    ASSERT_FALSE(found);

    // verify tableA appears in the deletion list
    vector<string> deletions;
    catalog->getDeletedPaths(deletions);
    vector<string>::iterator delIter;
    delIter = deletions.begin();
    found = false;
    while (delIter != deletions.end()) {
        string item = *delIter;
        if (item == "/clusters#cluster/databases#database/tables#tableA") {
            found = true;
        }
        ++delIter;
    }
    ASSERT_TRUE(found);

    // call this twice on purpose - reasonable to expect idempotent behaviour.
    catalog->purgeDeletions();
    catalog->purgeDeletions();
}

/*
 * Test on catalog.
 * Verify that subsequent execute() calls clear the wasAdded flags
 * from previous execute() calls.
 */
TEST_F(AddDropTableTest, WasAddedFlagCleared)
{
    catalog::Catalog *catalog = m_engine->getCatalog();
    catalog->execute(tableACmds());
    catalog->execute(tableBCmds());

    catalog::Cluster *cluster = catalog->clusters().get("cluster");
    catalog::Database *db = cluster->databases().get("database");

    ASSERT_EQ(2, db->tables().size());

    map<string, catalog::Table*>::const_iterator t_iter;
    for (t_iter = db->tables().begin();
         t_iter != db->tables().end();
         t_iter++)
    {
        catalog::Table *t = t_iter->second;
        if (t->name() == "tableA") {
            ASSERT_FALSE(t->wasAdded());
        }
        else if (t->name() == "tableB") {
            ASSERT_TRUE(t->wasAdded());
        }
    }
}

TEST_F(AddDropTableTest, DeletionsSetCleared)
{
    vector<std::string> deletions;
    vector<std::string>::iterator delIter;

    catalog::Catalog *catalog = m_engine->getCatalog();
    catalog::Cluster *cluster = catalog->clusters().get("cluster");
    catalog::Database *db = cluster->databases().get("database");

    catalog->execute(tableACmds());
    catalog->execute(tableBCmds());

    // delete a table. verify deletion bookkeeping
    catalog->execute(tableADeleteCmd());
    ASSERT_EQ(1, db->tables().size());
    catalog->getDeletedPaths(deletions);
    ASSERT_EQ(1, deletions.size());
    delIter = deletions.begin();
    while (delIter != deletions.end()) {
        string path = *delIter;
        ASSERT_EQ(path, "/clusters#cluster/databases#database/tables#tableA");
        ++delIter;
    }
    catalog->purgeDeletions();

    // delete a second table. verify deletion bookkeeping
    catalog->execute(tableBDeleteCmd());
    ASSERT_EQ(0, db->tables().size());
    deletions.clear();
    catalog->getDeletedPaths(deletions);
    ASSERT_EQ(1, deletions.size());
    delIter = deletions.begin();
    while (delIter != deletions.end()) {
        string path = *delIter;
        ASSERT_EQ(path, "/clusters#cluster/databases#database/tables#tableB");
        ++delIter;
    }
    catalog->purgeDeletions();
}

/*
 * Test on engine.
 * Verify updateCatalog adds table to engine's collections.
 */
TEST_F(AddDropTableTest, AddTable)
{
    bool changeResult = m_engine->updateCatalog( 0, tableACmds());
    ASSERT_TRUE(changeResult);

    Table *table1, *table2;
    table1 = m_engine->getTable("tableA");
    ASSERT_TRUE(table1 != NULL);

    table2 = m_engine->getTable(1); // catalogId
    ASSERT_TRUE(table2 != NULL);
    ASSERT_TRUE(table1 == table2);
}

/*
 * Test on engine.
 * Add two tables at once!
 */
TEST_F(AddDropTableTest, AddTwoTablesDropTwoTables)
{
    Table *table1, *table2;

    catalog::Catalog *catalog = m_engine->getCatalog();
    catalog::Cluster *cluster = catalog->clusters().get("cluster");
    catalog::Database *db = cluster->databases().get("database");
    ASSERT_EQ(0, db->tables().size());

    // add tableA, tableB
    std::string a_and_b = tableACmds() + "\n" + tableBCmds();
    bool changeResult = m_engine->updateCatalog( 0, a_and_b);
    ASSERT_TRUE(changeResult);
    ASSERT_EQ(2, db->tables().size());

    // verify first table
    table1 = m_engine->getTable("tableA");
    ASSERT_TRUE(table1 != NULL);

    table2 = m_engine->getTable(1); // catalogId
    ASSERT_TRUE(table2 != NULL);
    ASSERT_TRUE(table1 == table2);

    // verify second table
    table1 = m_engine->getTable("tableB");
    ASSERT_TRUE(table1 != NULL);

    table2 = m_engine->getTable(2); // catalogId
    ASSERT_TRUE(table2 != NULL);
    ASSERT_TRUE(table1 == table2);

    // drop tableA, tableB and verify
    table1->incrementRefcount();
    table2->incrementRefcount();

    std::string drop = tableADeleteCmd() + "\n" + tableBDeleteCmd();
    changeResult = m_engine->updateCatalog( 1,drop);
    ASSERT_TRUE(changeResult);
    ASSERT_EQ(0, db->tables().size());
    ASSERT_EQ(NULL, m_engine->getTable(1)); // catalogId
    ASSERT_EQ(NULL, m_engine->getTable("tableA"));
    ASSERT_EQ(NULL, m_engine->getTable(2)); // catalogId
    ASSERT_EQ(NULL, m_engine->getTable("tableB"));

    table1->decrementRefcount();
    table2->decrementRefcount();
}

/*
 * Test on engine.
 * Verify updateCatalog removes a table from engine's collections.
 */
TEST_F(AddDropTableTest, DropTable)
{
    // add. verified by AddTable test.
    bool result = m_engine->updateCatalog( 0, tableACmds());
    ASSERT_TRUE(result);

    Table *table1, *table2;

    // grab the table. need some data from it to complete the
    // test. hold a reference to keep it safe.
    table1 = m_engine->getTable("tableA");
    table1->incrementRefcount();

    ASSERT_TRUE(table1 != NULL);

    // and delete
    result = m_engine->updateCatalog( 1, tableADeleteCmd());
    ASSERT_TRUE(result);

    table2 = m_engine->getTable("tableA");
    ASSERT_TRUE(table2 == NULL);

    table2 = m_engine->getTable(0);
    ASSERT_TRUE(table2 == NULL);

    // release the last reference.
    table1->decrementRefcount();
}

/*
 * Add / Drop / Add
 */
TEST_F(AddDropTableTest, AddDropAdd)
{
    bool result;

    // std::string addboth = tableACmds() + "\n" + tableBCmds();
    // std::string dropboth = tableADeleteCmd() + "\n" + tableBDeleteCmd();

    // result = m_engine->updateCatalog(addboth, ++m_catVersion);
    // ASSERT_TRUE(result);

    result = m_engine->updateCatalog( -1, tableACmds());
    ASSERT_TRUE(result);

    for (int ii=0; ii < 20; ii++) {
        // result = m_engine->updateCatalog(tableBDeleteCmd(), ++m_catVersion);
        // ASSERT_TRUE(result);

        // A-only to B-only
        result = m_engine->updateCatalog( (ii * 2), tableADeleteCmd() + "\n" + tableBCmds());
        ASSERT_TRUE(result);

        // B-only to A-only
        result = m_engine->updateCatalog( (ii * 2) + 1, tableBDeleteCmd() + "\n" + tableACmds());
        ASSERT_TRUE(result);

        // result = m_engine->updateCatalog(tableBCmds(), ++m_catVersion);
        // ASSERT_TRUE(result);
    }
}

/*
 * Test on engine.
 * Verify updateCatalog removes a table from engine's collections.
 * And that stats are functional afterwards.
 */
TEST_F(AddDropTableTest, StatsWithDropTable)
{
    bool result = m_engine->updateCatalog( 0, tableACmds());
    ASSERT_TRUE(result);

    result = m_engine->updateCatalog( 1, tableBCmds());
    ASSERT_TRUE(result);

    // get stats - relying on valgrind for most verification here
    int locators12[] = {1, 2};
    int statresult = m_engine->getStats(STATISTICS_SELECTOR_TYPE_TABLE, locators12, 2, false, 1L);
    ASSERT_TRUE(statresult == 1);

    // delete A.
    result = m_engine->updateCatalog( 2, tableADeleteCmd());
    ASSERT_TRUE(result);

    // get stats for the remaining table by relative offset
    int locators1[] = {1};
    statresult = m_engine->getStats(STATISTICS_SELECTOR_TYPE_TABLE, locators1, 1, false, 1L);
    ASSERT_TRUE(statresult == 1);

    result = m_engine->updateCatalog( 3, tableACmds());
    ASSERT_TRUE(result);

    // get stats for the tables by relative offset
    statresult = m_engine->getStats(STATISTICS_SELECTOR_TYPE_TABLE, locators12, 2, false, 1L);
    ASSERT_TRUE(statresult == 1);
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
