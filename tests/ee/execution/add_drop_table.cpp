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


#include "harness.h"
#include "common/common.h"
#include "execution/VoltDBEngine.h"

using namespace voltdb;

class AddDropTableTest : public Test {

  public:
    AddDropTableTest()
        : m_clusterId(0), m_databaseId(0), m_siteId(0), m_partitionId(0),
          m_hostId(101), m_hostName("host101")
    {
        m_engine = new VoltDBEngine();
        m_engine->initialize(m_clusterId, m_siteId, m_partitionId,
                             m_hostId, m_hostName);

        std::string initialCatalog =
          "add / clusters cluster\n"
          "add /clusters[cluster] databases database\n"
          "add /clusters[cluster]/databases[database] programs program\n"
          "add /clusters[cluster] hosts 0\n"
          "add /clusters[cluster] partitions 0\n"
          "add /clusters[cluster] partitions 1\n"
          "add /clusters[cluster] partitions 2\n"
          "add /clusters[cluster] sites 0\n"
          "set /clusters[cluster]/sites[0] partition /clusters[cluster]/partitions[0]\n"
          "set /clusters[cluster]/sites[0] host /clusters[cluster]/hosts[0]";

        bool loadResult = m_engine->loadCatalog(initialCatalog);
        ASSERT_TRUE(loadResult);
    }

    ~AddDropTableTest()
    {
        delete m_engine;
    }


    std::string tableACmds()
    {
        return
          "add /clusters[cluster]/databases[database] tables tableA\n"
          "set /clusters[cluster]/databases[database]/tables[tableA] type 0\n"
          "set /clusters[cluster]/databases[database]/tables[tableA] isreplicated false\n"
          "set /clusters[cluster]/databases[database]/tables[tableA] partitioncolumn 0\n"
          "set /clusters[cluster]/databases[database]/tables[tableA] estimatedtuplecount 0\n"
          "add /clusters[cluster]/databases[database]/tables[tableA] columns A\n"
          "set /clusters[cluster]/databases[database]/tables[tableA]/columns[A] index 0\n"
          "set /clusters[cluster]/databases[database]/tables[tableA]/columns[A] type 5\n"
          "set /clusters[cluster]/databases[database]/tables[tableA]/columns[A] size 0\n"
          "set /clusters[cluster]/databases[database]/tables[tableA]/columns[A] nullable false\n"
          "set /clusters[cluster]/databases[database]/tables[tableA]/columns[A] name \"A\"";
    }

  protected:
    CatalogId m_clusterId;
    CatalogId m_databaseId;
    CatalogId m_siteId;
    CatalogId m_partitionId;
    CatalogId m_hostId;
    std::string m_hostName;

    VoltDBEngine *m_engine;
};

TEST_F(AddDropTableTest, AddTable)
{
    bool changeResult = m_engine->updateCatalog(tableACmds());
    ASSERT_TRUE(changeResult);

    Table *table = m_engine->getTable("tableA");
    ASSERT_TRUE(table != NULL);
}


int main() {
    return TestSuite::globalInstance()->runAll();
}

