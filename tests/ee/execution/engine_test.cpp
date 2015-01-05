/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include <cstdlib>
#include <ctime>
#include <unistd.h>
#include <boost/shared_ptr.hpp>
#include "harness.h"
#include "common/common.h"
#include "expressions/abstractexpression.h"
#include "common/valuevector.h"
#include "common/tabletuple.h"
#include "execution/VoltDBEngine.h"
#include "plannodes/abstractplannode.h"
#include "indexes/tableindex.h"
#include "storage/table.h"
#include "storage/tablefactory.h"
#include "storage/tableiterator.h"
#include "storage/temptable.h"
#include "storage/tableutil.h"
#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "catalog/table.h"
#include "catalog/database.h"
#include "catalog/constraint.h"

using namespace std;
using namespace voltdb;

#define NUM_OF_COLUMNS 5
#define NUM_OF_INDEXES 2
#define NUM_OF_TUPLES 10 //must be multiples of 2 for Update test.

//
// Define the column information for the main test table
// This is useful because it will allow us to check different types and other
// configurations without having to dig down into the code
//
ValueType COLUMN_TYPES[NUM_OF_COLUMNS]  = { VALUE_TYPE_BIGINT,
                                                    VALUE_TYPE_BIGINT,
                                                    VALUE_TYPE_BIGINT,
                                                    VALUE_TYPE_BIGINT,
                                                    VALUE_TYPE_BIGINT };
int COLUMN_SIZES[NUM_OF_COLUMNS]                = { 8, 8, 8, 8, 8};
bool COLUMN_ALLOW_NULLS[NUM_OF_COLUMNS]         = { true, true, true, true, true };

class ExecutionEngineTest : public Test {
    public:
        ExecutionEngineTest() {
            srand((unsigned int)time(NULL));
            catalog_string = "add / clusters cluster"
                "\nadd /clusters[cluster databases database"
                "\nset /clusters[cluster/databases[database schema \"435245415445205441424C452057415245484F5553452028575F494420494E54454745522044454641554C5420273027204E4F54204E554C4C2C0A575F4E414D452056415243484152283136292044454641554C54204E554C4C2C0A5052494D415259204B45592028575F4944290A293B0A20435245415445205441424C452053544F434B2028535F495F494420494E5445474552204E4F54204E554C4C2C0A535F575F494420494E5445474552204E4F54204E554C4C2C0A535F5155414E5449545920494E5445474552204E4F54204E554C4C2C0A5052494D415259204B45592028535F495F4944290A293B0A20\""
                "\nadd /clusters[cluster/databases[database programs program"
                "\nadd /clusters[cluster/databases[database tables WAREHOUSE"
                "\nset /clusters[cluster/databases[database/tables[WAREHOUSE type 0"
                "\nset /clusters[cluster/databases[database/tables[WAREHOUSE isreplicated false"
                "\nset /clusters[cluster/databases[database/tables[WAREHOUSE partitioncolumn 0"
                "\nset /clusters[cluster/databases[database/tables[WAREHOUSE estimatedtuplecount 0"
                "\nadd /clusters[cluster/databases[database/tables[WAREHOUSE columns W_ID"
                "\nset /clusters[cluster/databases[database/tables[WAREHOUSE/columns[W_ID index 0"
                "\nset /clusters[cluster/databases[database/tables[WAREHOUSE/columns[W_ID type 5"
                "\nset /clusters[cluster/databases[database/tables[WAREHOUSE/columns[W_ID size 0"
                "\nset /clusters[cluster/databases[database/tables[WAREHOUSE/columns[W_ID nullable false"
                "\nset /clusters[cluster/databases[database/tables[WAREHOUSE/columns[W_ID name \"W_ID\""
                "\nadd /clusters[cluster/databases[database/tables[WAREHOUSE columns W_NAME"
                "\nset /clusters[cluster/databases[database/tables[WAREHOUSE/columns[W_NAME index 1"
                "\nset /clusters[cluster/databases[database/tables[WAREHOUSE/columns[W_NAME type 9"
                "\nset /clusters[cluster/databases[database/tables[WAREHOUSE/columns[W_NAME size 16"
                "\nset /clusters[cluster/databases[database/tables[WAREHOUSE/columns[W_NAME nullable true"
                "\nset /clusters[cluster/databases[database/tables[WAREHOUSE/columns[W_NAME name \"W_NAME\""
                "\nadd /clusters[cluster/databases[database tables STOCK"
                "\nset /clusters[cluster/databases[database/tables[STOCK type 0"
                "\nset /clusters[cluster/databases[database/tables[STOCK isreplicated false"
                "\nset /clusters[cluster/databases[database/tables[STOCK partitioncolumn 0"
                "\nset /clusters[cluster/databases[database/tables[STOCK estimatedtuplecount 0"
                "\nadd /clusters[cluster/databases[database/tables[STOCK columns S_I_ID"
                "\nset /clusters[cluster/databases[database/tables[STOCK/columns[S_I_ID index 0"
                "\nset /clusters[cluster/databases[database/tables[STOCK/columns[S_I_ID type 5"
                "\nset /clusters[cluster/databases[database/tables[STOCK/columns[S_I_ID size 0"
                "\nset /clusters[cluster/databases[database/tables[STOCK/columns[S_I_ID nullable false"
                "\nset /clusters[cluster/databases[database/tables[STOCK/columns[S_I_ID name \"S_I_ID\""
                "\nadd /clusters[cluster/databases[database/tables[STOCK columns S_W_ID"
                "\nset /clusters[cluster/databases[database/tables[STOCK/columns[S_W_ID index 1"
                "\nset /clusters[cluster/databases[database/tables[STOCK/columns[S_W_ID type 5"
                "\nset /clusters[cluster/databases[database/tables[STOCK/columns[S_W_ID size 0"
                "\nset /clusters[cluster/databases[database/tables[STOCK/columns[S_W_ID nullable false"
                "\nset /clusters[cluster/databases[database/tables[STOCK/columns[S_W_ID name \"S_W_ID\""
                "\nadd /clusters[cluster/databases[database/tables[STOCK columns S_QUANTITY"
                "\nset /clusters[cluster/databases[database/tables[STOCK/columns[S_QUANTITY index 2"
                "\nset /clusters[cluster/databases[database/tables[STOCK/columns[S_QUANTITY type 5"
                "\nset /clusters[cluster/databases[database/tables[STOCK/columns[S_QUANTITY size 0"
                "\nset /clusters[cluster/databases[database/tables[STOCK/columns[S_QUANTITY nullable false"
                "\nset /clusters[cluster/databases[database/tables[STOCK/columns[S_QUANTITY name \"S_QUANTITY\"";

            /*
             * Initialize the engine
             */
            engine = new VoltDBEngine();
            int partitionCount = 3;
            ASSERT_TRUE(engine->initialize(this->cluster_id, this->site_id, 0, 0, "", DEFAULT_TEMP_TABLE_MEMORY));
            engine->updateHashinator( HASHINATOR_LEGACY, (char*)&partitionCount, NULL, 0);
            ASSERT_TRUE(engine->loadCatalog( -2, catalog_string));

            /*
             * Get a link to the catalog and pull out information about it
             */
            catalog = engine->getCatalog();
            cluster = catalog->clusters().get("cluster");
            database = cluster->databases().get("database");
            database_id = database->relativeIndex();
            catalog::Table *catalog_table_warehouse = database->tables().get("WAREHOUSE");
            catalog::Table *catalog_table_stock = database->tables().get("STOCK");
            warehouse_table_id = catalog_table_warehouse->relativeIndex();
            stock_table_id = catalog_table_stock->relativeIndex();
            warehouse_table = engine->getTable(warehouse_table_id);
            stock_table = engine->getTable(stock_table_id);
            //
            // Fill in tuples
            //
            ASSERT_TRUE(warehouse_table);
            ASSERT_TRUE(stock_table);
            ASSERT_TRUE(tableutil::addRandomTuples(warehouse_table, NUM_OF_TUPLES));
            ASSERT_TRUE(tableutil::addRandomTuples(stock_table, NUM_OF_TUPLES));
        }
        ~ExecutionEngineTest() {
            //
            // We just need to delete the VoltDBEngine
            // It will cleanup all the tables for us
            //
            delete(this->engine);
        }

    protected:
        CatalogId cluster_id;
        CatalogId database_id;
        CatalogId site_id;
        VoltDBEngine *engine;
        string catalog_string;
        catalog::Catalog *catalog; //This is not the real catalog that the VoltDBEngine uses. It is a duplicate made locally to get GUIDs
        catalog::Cluster *cluster;
        catalog::Database *database;
        catalog::Constraint *constraint;

        Table* warehouse_table;
        Table* stock_table;

        int warehouse_table_id;
        int stock_table_id;

        void compareTables(Table *first, Table* second);
};

/*
// ------------------------------------------------------------------
// Execute_PlanFragmentInfo
// ------------------------------------------------------------------
TEST_F(ExecutionEngineTest, Execute_PlanFragmentInfo) {
    //
    // Given a PlanFragmentInfo data object, make the engine executes it properly
    //
}

// ------------------------------------------------------------------
// Execute_PlanFragmentId
// ------------------------------------------------------------------
TEST_F(ExecutionEngineTest, Execute_PlanFragmentId) {
    //
    // Given a PlanFragmentInfo data object, make sure that the engine converts it to
    // PlanFragmentInfo object and executes it properly
    //
}

// ------------------------------------------------------------------
// Execute_PlanFragmentPayload
// ------------------------------------------------------------------
TEST_F(ExecutionEngineTest, Execute_PlanFragmentPayload) {
    //
    // Given a PlanFragmentInfo payload object, make sure that the engine deserializes
    // it into a PlanFragmentInfo object and executes it properly
    //
}

// ------------------------------------------------------------------
// Send
// ------------------------------------------------------------------
TEST_F(ExecutionEngineTest, Send) {
    //
    // Not sure what this will do just yet...
    //
}

// ------------------------------------------------------------------
// Receive_Table
// ------------------------------------------------------------------
TEST_F(ExecutionEngineTest, Receive_Table) {
    //
    // Not sure what this will do just yet...
    //
}

// ------------------------------------------------------------------
// Receive_TablePointer
// ------------------------------------------------------------------
TEST_F(ExecutionEngineTest, Receive_TablePointer) {
    //
    // Not sure what this will do just yet...
    //
}

// ------------------------------------------------------------------
// Receive_TablePayload
// ------------------------------------------------------------------
TEST_F(ExecutionEngineTest, Receive_TablePayload) {
    //
    // Not sure what this will do just yet...
    //
}
*/
int main() {
     return TestSuite::globalInstance()->runAll();
}
