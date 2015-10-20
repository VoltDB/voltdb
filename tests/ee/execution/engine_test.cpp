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

#define NUM_OF_COLUMNS 4
#define NUM_OF_INDEXES 3
#define NUM_OF_TUPLES 10 //must be multiples of 2 for Update test.

//
// Define the column information for the main test table
// This is useful because it will allow us to check different types and other
// configurations without having to dig down into the code
//
ValueType COLUMN_TYPES[NUM_OF_COLUMNS]  = { VALUE_TYPE_INTEGER,
                                                    VALUE_TYPE_VARCHAR,
                                                    VALUE_TYPE_VARCHAR,
                                                    VALUE_TYPE_INTEGER };
int COLUMN_SIZES[NUM_OF_COLUMNS]                = { 4, 8, 8, 4};
bool COLUMN_ALLOW_NULLS[NUM_OF_COLUMNS]         = { false, true, true, false };

class ExecutionEngineTest : public Test {
    public:
        ExecutionEngineTest() {
            srand((unsigned int)time(NULL));
            catalog_string =
                    "add / clusters cluster"
                    "\nset /clusters#cluster localepoch 1199145600"
                    "\nadd /clusters#cluster databases database"
                    "\nset /clusters#cluster/databases#database schema \"eJydUVuOw0AI+9/TMMYwM59tOrn/kWqiSlW7UbdaRRoID9sAPcBgCzJgehvBrfz0Ht07c89bRgcMA2bT5nuGk4TpXYXCzghVc3GPiq8IIW5Ct6jam9gQU5mrKie2B2rm7DhwV7ZCVkdWLZ2jLIY3hw88e7Zsrx09FUF6jrK/O/pFLJpN2ln1f6p+9Mkv5bNyYccErD9Fdy7UZHv0cJgYrb7qxPTrD19WXISUF1qmhLeEhghOjSBBXYJVc0CeH+B8+SdE0r+ksMl+Q4tPtKekmu/kZhV9v8o/tuAf5XxPewcEnpp/\""
                    "\nset $PREV isActiveActiveDRed false"
                    "\nset $PREV securityprovider \"hash\""
                    "\nadd /clusters#cluster/databases#database groups administrator"
                    "\nset /clusters#cluster/databases#database/groups#administrator admin true"
                    "\nadd /clusters#cluster/databases#database groups user"
                    "\nset /clusters#cluster/databases#database/groups#user admin false"
                    "\nadd /clusters#cluster/databases#database tables CUSTOMER"
                    "\nset /clusters#cluster/databases#database/tables#CUSTOMER isreplicated false"
                    "\nset $PREV partitioncolumn /clusters#cluster/databases#database/tables#CUSTOMER/columns#CUSTOMERID"
                    "\nset $PREV estimatedtuplecount 0"
                    "\nset $PREV materializer null"
                    "\nset $PREV signature \"CUSTOMER|ivvi\""
                    "\nset $PREV tuplelimit 1000"
                    "\nset $PREV isDRed false"
                    "\nadd /clusters#cluster/databases#database/tables#CUSTOMER columns CUSTOMERID"
                    "\nset /clusters#cluster/databases#database/tables#CUSTOMER/columns#CUSTOMERID index 0"
                    "\nset $PREV type 5"
                    "\nset $PREV size 4"
                    "\nset $PREV nullable false"
                    "\nset $PREV name \"CUSTOMERID\""
                    "\nset $PREV defaultvalue null"
                    "\nset $PREV defaulttype 0"
                    "\nset $PREV matview null"
                    "\nset $PREV aggregatetype 0"
                    "\nset $PREV matviewsource null"
                    "\nset $PREV inbytes false"
                    "\nadd /clusters#cluster/databases#database/tables#CUSTOMER columns FIRSTNAME"
                    "\nset /clusters#cluster/databases#database/tables#CUSTOMER/columns#FIRSTNAME index 1"
                    "\nset $PREV type 9"
                    "\nset $PREV size 128"
                    "\nset $PREV nullable true"
                    "\nset $PREV name \"FIRSTNAME\""
                    "\nset $PREV defaultvalue null"
                    "\nset $PREV defaulttype 0"
                    "\nset $PREV matview null"
                    "\nset $PREV aggregatetype 0"
                    "\nset $PREV matviewsource null"
                    "\nset $PREV inbytes false"
                    "\nadd /clusters#cluster/databases#database/tables#CUSTOMER columns LASTNAME"
                    "\nset /clusters#cluster/databases#database/tables#CUSTOMER/columns#LASTNAME index 2"
                    "\nset $PREV type 9"
                    "\nset $PREV size 128"
                    "\nset $PREV nullable true"
                    "\nset $PREV name \"LASTNAME\""
                    "\nset $PREV defaultvalue null"
                    "\nset $PREV defaulttype 0"
                    "\nset $PREV matview null"
                    "\nset $PREV aggregatetype 0"
                    "\nset $PREV matviewsource null"
                    "\nset $PREV inbytes false"
                    "\nadd /clusters#cluster/databases#database/tables#CUSTOMER columns ZIPCODE"
                    "\nset /clusters#cluster/databases#database/tables#CUSTOMER/columns#ZIPCODE index 3"
                    "\nset $PREV type 5"
                    "\nset $PREV size 4"
                    "\nset $PREV nullable false"
                    "\nset $PREV name \"ZIPCODE\""
                    "\nset $PREV defaultvalue null"
                    "\nset $PREV defaulttype 0"
                    "\nset $PREV matview null"
                    "\nset $PREV aggregatetype 0"
                    "\nset $PREV matviewsource null"
                    "\nset $PREV inbytes false"
                    "\nadd /clusters#cluster/databases#database/tables#CUSTOMER indexes TABLEINDEX1"
                    "\nset /clusters#cluster/databases#database/tables#CUSTOMER/indexes#TABLEINDEX1 unique false"
                    "\nset $PREV assumeUnique false"
                    "\nset $PREV countable true"
                    "\nset $PREV type 1"
                    "\nset $PREV expressionsjson \"\""
                    "\nset $PREV predicatejson \"\""
                    "\nadd /clusters#cluster/databases#database/tables#CUSTOMER/indexes#TABLEINDEX1 columns CUSTOMERID"
                    "\nset /clusters#cluster/databases#database/tables#CUSTOMER/indexes#TABLEINDEX1/columns#CUSTOMERID index 0"
                    "\nset $PREV column /clusters#cluster/databases#database/tables#CUSTOMER/columns#CUSTOMERID"
                    "\nadd /clusters#cluster/databases#database/tables#CUSTOMER indexes TABLEINDEX2"
                    "\nset /clusters#cluster/databases#database/tables#CUSTOMER/indexes#TABLEINDEX2 unique true"
                    "\nset $PREV assumeUnique false"
                    "\nset $PREV countable true"
                    "\nset $PREV type 1"
                    "\nset $PREV expressionsjson \"\""
                    "\nset $PREV predicatejson \"\""
                    "\nadd /clusters#cluster/databases#database/tables#CUSTOMER/indexes#TABLEINDEX2 columns CUSTOMERID"
                    "\nset /clusters#cluster/databases#database/tables#CUSTOMER/indexes#TABLEINDEX2/columns#CUSTOMERID index 0"
                    "\nset $PREV column /clusters#cluster/databases#database/tables#CUSTOMER/columns#CUSTOMERID"
                    "\nadd /clusters#cluster/databases#database/tables#CUSTOMER/indexes#TABLEINDEX2 columns FIRSTNAME"
                    "\nset /clusters#cluster/databases#database/tables#CUSTOMER/indexes#TABLEINDEX2/columns#FIRSTNAME index 1"
                    "\nset $PREV column /clusters#cluster/databases#database/tables#CUSTOMER/columns#FIRSTNAME"
                    "\nadd /clusters#cluster/databases#database/tables#CUSTOMER/indexes#TABLEINDEX2 columns LASTNAME"
                    "\nset /clusters#cluster/databases#database/tables#CUSTOMER/indexes#TABLEINDEX2/columns#LASTNAME index 2"
                    "\nset $PREV column /clusters#cluster/databases#database/tables#CUSTOMER/columns#LASTNAME"
                    "\nadd /clusters#cluster/databases#database/tables#CUSTOMER indexes TABLEINDEX3"
                    "\nset /clusters#cluster/databases#database/tables#CUSTOMER/indexes#TABLEINDEX3 unique false"
                    "\nset $PREV assumeUnique false"
                    "\nset $PREV countable true"
                    "\nset $PREV type 1"
                    "\nset $PREV expressionsjson \"\""
                    "\nset $PREV predicatejson \"\""
                    "\nadd /clusters#cluster/databases#database/tables#CUSTOMER/indexes#TABLEINDEX3 columns FIRSTNAME"
                    "\nset /clusters#cluster/databases#database/tables#CUSTOMER/indexes#TABLEINDEX3/columns#FIRSTNAME index 0"
                    "\nset $PREV column /clusters#cluster/databases#database/tables#CUSTOMER/columns#FIRSTNAME"
                    "\nadd /clusters#cluster/databases#database/tables#CUSTOMER/indexes#TABLEINDEX3 columns LASTNAME"
                    "\nset /clusters#cluster/databases#database/tables#CUSTOMER/indexes#TABLEINDEX3/columns#LASTNAME index 1"
                    "\nset $PREV column /clusters#cluster/databases#database/tables#CUSTOMER/columns#LASTNAME"
                    "\nadd /clusters#cluster/databases#database/tables#CUSTOMER indexes VOLTDB_AUTOGEN_IDX_PK_CUSTOMER_CUSTOMERID"
                    "\nset /clusters#cluster/databases#database/tables#CUSTOMER/indexes#VOLTDB_AUTOGEN_IDX_PK_CUSTOMER_CUSTOMERID unique true"
                    "\nset $PREV assumeUnique false"
                    "\nset $PREV countable true"
                    "\nset $PREV type 1"
                    "\nset $PREV expressionsjson \"\""
                    "\nset $PREV predicatejson \"\""
                    "\nadd /clusters#cluster/databases#database/tables#CUSTOMER/indexes#VOLTDB_AUTOGEN_IDX_PK_CUSTOMER_CUSTOMERID columns CUSTOMERID"
                    "\nset /clusters#cluster/databases#database/tables#CUSTOMER/indexes#VOLTDB_AUTOGEN_IDX_PK_CUSTOMER_CUSTOMERID/columns#CUSTOMERID index 0"
                    "\nset $PREV column /clusters#cluster/databases#database/tables#CUSTOMER/columns#CUSTOMERID"
                    "\nadd /clusters#cluster/databases#database/tables#CUSTOMER constraints VOLTDB_AUTOGEN_IDX_PK_CUSTOMER_CUSTOMERID"
                    "\nset /clusters#cluster/databases#database/tables#CUSTOMER/constraints#VOLTDB_AUTOGEN_IDX_PK_CUSTOMER_CUSTOMERID type 4"
                    "\nset $PREV oncommit \"\""
                    "\nset $PREV index /clusters#cluster/databases#database/tables#CUSTOMER/indexes#VOLTDB_AUTOGEN_IDX_PK_CUSTOMER_CUSTOMERID"
                    "\nset $PREV foreignkeytable null";

            /*
             * Initialize the engine
             */
            engine = new VoltDBEngine();
            int partitionCount = 3;
            ASSERT_TRUE(engine->initialize(this->cluster_id, this->site_id, 0, 0, "", 0, DEFAULT_TEMP_TABLE_MEMORY, false));
            engine->updateHashinator( HASHINATOR_LEGACY, (char*)&partitionCount, NULL, 0);
            ASSERT_TRUE(engine->loadCatalog( -2, catalog_string));

            /*
             * Get a link to the catalog and pull out information about it
             */
            catalog = engine->getCatalog();
            cluster = catalog->clusters().get("cluster");
            database = cluster->databases().get("database");
            database_id = database->relativeIndex();
            catalog::Table *catalog_table_customer = database->tables().get("CUSTOMER");
            customer_table_id = catalog_table_customer->relativeIndex();
            customer_table = engine->getTable(customer_table_id);
            //
            // Fill in tuples
            //
            ASSERT_TRUE(customer_table);
            ASSERT_TRUE(tableutil::addRandomTuples(customer_table, NUM_OF_TUPLES));
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

        Table* customer_table;

        int customer_table_id;

        void compareTables(Table *first, Table* second);
};

/* Check the order of index vector
 * Index vector should follow the order of primary key first, all unique indices afterwards, and all the non-unique indices at the end.
 */
TEST_F(ExecutionEngineTest, IndexOrder) {
    ASSERT_TRUE(customer_table->primaryKeyIndex() == customer_table->allIndexes()[0]);
    ASSERT_TRUE(customer_table->allIndexes()[1]->isUniqueIndex());
    ASSERT_FALSE(customer_table->allIndexes()[2]->isUniqueIndex());
    ASSERT_FALSE(customer_table->allIndexes()[3]->isUniqueIndex());
}

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
