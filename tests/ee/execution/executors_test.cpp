/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB L.L.C. are licensed under the following
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

#include <iostream>
#include <cstdlib>
#include <stdint.h>
#include <ctime>
#include <boost/shared_ptr.hpp>
#include "harness.h"
#include "common/common.h"
#include "common/NValue.hpp"
#include "common/ValuePeeker.hpp"
#include "common/ValueFactory.hpp"
#include "common/valuevector.h"
#include "common/types.h"
#include "common/tabletuple.h"
#include "expressions/abstractexpression.h"
#include "expressions/expressions.h"
#include "expressions/expressionutil.h"
#include "execution/VoltDBEngine.h"
#include "executors/executors.h"
#include "executors/executorutil.h"
#include "plannodes/nodes.h"
#include "plannodes/abstractplannode.h"
#include "indexes/tableindex.h"
#include "storage/table.h"
#include "storage/persistenttable.h"
#include "storage/tablefactory.h"
#include "storage/tableiterator.h"
#include "storage/temptable.h"
#include "storage/tableutil.h"
#include "execution/VoltDBEngine.h"
#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "catalog/table.h"
#include "catalog/database.h"

using std::string;
using std::vector;
using namespace voltdb;

#define NUM_OF_COLUMNS 5
#define NUM_OF_INDEXES 3
#define NUM_OF_TUPLES 10 //must be multiples of 2 for Update test.

//
// Define the column information for the main test table
// This is useful because it will allow us to check different types and other
// configurations without having to dig down into the code
//
voltdb::ValueType COLUMN_TYPES[NUM_OF_COLUMNS] = { voltdb::VALUE_TYPE_BIGINT,
        voltdb::VALUE_TYPE_BIGINT,
        voltdb::VALUE_TYPE_BIGINT,
        voltdb::VALUE_TYPE_BIGINT,
        voltdb::VALUE_TYPE_BIGINT };
short COLUMN_SIZES[NUM_OF_COLUMNS]             = { 8, 8, 8, 8, 8 };
bool COLUMN_ALLOW_NULLS[NUM_OF_COLUMNS]        = { true, true, true, true, true };

//
// The indexes for the main test table
// For each index, we define a bitmap in INDEX_COLUMNS to signal whether a particular
// column should be included in that index
//
bool INDEX_COLUMNS[NUM_OF_INDEXES][NUM_OF_COLUMNS] = { {true, false, false, false, false},
        {true, true, false, false, false},
        {false, true, false, false, false} };
int INDEX_PRIMARY_KEY = 0; // the first index should be set as the primary key

class ExecutionPlanNodeTest : public Test {
public:
    ExecutionPlanNodeTest() : empty_params(0) {
        // srand((unsigned int)time(NULL)); PLEASE don't use indeterministic random seed in testcase.
        srand(0);
        catalog_string
        = "add / clusters cluster"
            "\nadd /clusters[cluster] databases database"
            "\nset /clusters[cluster]/databases[database] schema \"435245415445205441424C452057415245484F5553452028575F494420494E54454745522044454641554C5420273027204E4F54204E554C4C2C0A575F4E414D452056415243484152283136292044454641554C54204E554C4C2C0A5052494D415259204B45592028575F4944290A293B0A20435245415445205441424C452053544F434B2028535F495F494420494E5445474552204E4F54204E554C4C2C0A535F575F494420494E5445474552204E4F54204E554C4C2C0A535F5155414E5449545920494E5445474552204E4F54204E554C4C2C0A5052494D415259204B45592028535F495F4944290A293B0A20\""
            "\nadd /clusters[cluster]/databases[database] programs program"
            "\nadd /clusters[cluster]/databases[database] tables WAREHOUSE"
            "\nset /clusters[cluster]/databases[database]/tables[WAREHOUSE] type 0"
            "\nset /clusters[cluster]/databases[database]/tables[WAREHOUSE] isreplicated false"
            "\nset /clusters[cluster]/databases[database]/tables[WAREHOUSE] partitioncolumn 0"
            "\nset /clusters[cluster]/databases[database]/tables[WAREHOUSE] estimatedtuplecount 0"
            "\nadd /clusters[cluster]/databases[database]/tables[WAREHOUSE] columns W_ID"
            "\nset /clusters[cluster]/databases[database]/tables[WAREHOUSE]/columns[W_ID] index 0"
            "\nset /clusters[cluster]/databases[database]/tables[WAREHOUSE]/columns[W_ID] type 5"
            "\nset /clusters[cluster]/databases[database]/tables[WAREHOUSE]/columns[W_ID] size 0"
            "\nset /clusters[cluster]/databases[database]/tables[WAREHOUSE]/columns[W_ID] nullable false"
            "\nset /clusters[cluster]/databases[database]/tables[WAREHOUSE]/columns[W_ID] name \"W_ID\""
            "\nadd /clusters[cluster]/databases[database]/tables[WAREHOUSE] columns W_NAME"
            "\nset /clusters[cluster]/databases[database]/tables[WAREHOUSE]/columns[W_NAME] index 1"
            "\nset /clusters[cluster]/databases[database]/tables[WAREHOUSE]/columns[W_NAME] type 9"
            "\nset /clusters[cluster]/databases[database]/tables[WAREHOUSE]/columns[W_NAME] size 16"
            "\nset /clusters[cluster]/databases[database]/tables[WAREHOUSE]/columns[W_NAME] nullable true"
            "\nset /clusters[cluster]/databases[database]/tables[WAREHOUSE]/columns[W_NAME] name \"W_NAME\""
            "\nadd /clusters[cluster]/databases[database] tables STOCK"
            "\nset /clusters[cluster]/databases[database]/tables[STOCK] type 0"
            "\nset /clusters[cluster]/databases[database]/tables[STOCK] isreplicated false"
            "\nset /clusters[cluster]/databases[database]/tables[STOCK] partitioncolumn 0"
            "\nset /clusters[cluster]/databases[database]/tables[STOCK] estimatedtuplecount 0"
            "\nadd /clusters[cluster]/databases[database]/tables[STOCK] columns S_I_ID"
            "\nset /clusters[cluster]/databases[database]/tables[STOCK]/columns[S_I_ID] index 0"
            "\nset /clusters[cluster]/databases[database]/tables[STOCK]/columns[S_I_ID] type 6"
            "\nset /clusters[cluster]/databases[database]/tables[STOCK]/columns[S_I_ID] size 0"
            "\nset /clusters[cluster]/databases[database]/tables[STOCK]/columns[S_I_ID] nullable false"
            "\nset /clusters[cluster]/databases[database]/tables[STOCK]/columns[S_I_ID] name \"S_I_ID\""
            "\nadd /clusters[cluster]/databases[database]/tables[STOCK] columns C1"
            "\nset /clusters[cluster]/databases[database]/tables[STOCK]/columns[C1] index 1"
            "\nset /clusters[cluster]/databases[database]/tables[STOCK]/columns[C1] type 6"
            "\nset /clusters[cluster]/databases[database]/tables[STOCK]/columns[C1] size 0"
            "\nset /clusters[cluster]/databases[database]/tables[STOCK]/columns[C1] nullable false"
            "\nset /clusters[cluster]/databases[database]/tables[STOCK]/columns[C1] name \"C1\""
            "\nadd /clusters[cluster]/databases[database]/tables[STOCK] columns C2"
            "\nset /clusters[cluster]/databases[database]/tables[STOCK]/columns[C2] index 2"
            "\nset /clusters[cluster]/databases[database]/tables[STOCK]/columns[C2] type 6"
            "\nset /clusters[cluster]/databases[database]/tables[STOCK]/columns[C2] size 0"
            "\nset /clusters[cluster]/databases[database]/tables[STOCK]/columns[C2] nullable false"
            "\nset /clusters[cluster]/databases[database]/tables[STOCK]/columns[C2] name \"C2\""
            "\nadd /clusters[cluster]/databases[database]/tables[STOCK] columns C3"
            "\nset /clusters[cluster]/databases[database]/tables[STOCK]/columns[C3] index 3"
            "\nset /clusters[cluster]/databases[database]/tables[STOCK]/columns[C3] type 6"
            "\nset /clusters[cluster]/databases[database]/tables[STOCK]/columns[C3] size 0"
            "\nset /clusters[cluster]/databases[database]/tables[STOCK]/columns[C3] nullable false"
            "\nset /clusters[cluster]/databases[database]/tables[STOCK]/columns[C3] name \"C3\""
            "\nadd /clusters[cluster]/databases[database]/tables[STOCK] columns C4"
            "\nset /clusters[cluster]/databases[database]/tables[STOCK]/columns[C4] index 4"
            "\nset /clusters[cluster]/databases[database]/tables[STOCK]/columns[C4] type 6"
            "\nset /clusters[cluster]/databases[database]/tables[STOCK]/columns[C4] size 0"
            "\nset /clusters[cluster]/databases[database]/tables[STOCK]/columns[C4] nullable false"
            "\nset /clusters[cluster]/databases[database]/tables[STOCK]/columns[C4] name \"C4\"";

        catalog::Catalog tempCatalog; // To retrieve the cluster ID
        tempCatalog.execute(catalog_string);
        cluster_id = tempCatalog.clusters().get("cluster")->relativeIndex();
        constraint = tempCatalog.clusters().get("cluster")->databases().get(
                "database")->tables().get("STOCK")->constraints().get(
                        "SYS_PK_49");
        site_id = 1001; //This value doesn't matter

        /*
         * Initialize the engine
         */
        engine = new voltdb::VoltDBEngine();
        ASSERT_TRUE(engine->initialize(this->cluster_id, this->site_id));
        ASSERT_TRUE(engine->loadCatalog(catalog_string));

        /*
         * Get a link to the catalog and pull out information about it
         */
        catalog = engine->getCatalog();
        cluster = catalog->clusters().get("cluster");
        database = cluster->databases().get("database");
        database_id = database->relativeIndex();
        catalog::Table *catalog_table_warehouse = database->tables().get(
                "WAREHOUSE");
        catalog::Table *catalog_table_stock = database->tables().get("STOCK");
        warehouse_table_id = catalog_table_warehouse->relativeIndex();
        stock_table_id = catalog_table_stock->relativeIndex();
        warehouse_table = engine->getTable(warehouse_table_id);
        stock_table = engine->getTable(stock_table_id);

        //
        // Fill in tuples
        //
        ASSERT_TRUE(tableutil::addRandomTuples(warehouse_table, NUM_OF_TUPLES));
        ASSERT_TRUE(tableutil::addRandomTuples(stock_table, NUM_OF_TUPLES));
        this->xact_id = 1;
        this->table_name = "test_table";

        assert(this->init());
    }
    ~ExecutionPlanNodeTest() {
        //
        // We just need to delete the VoltDBEngine
        // It will cleanup all the tables for us
        //
        delete(this->engine);
        delete(this->table);
    }

protected:
    voltdb::CatalogId cluster_id;
    voltdb::CatalogId database_id;
    voltdb::CatalogId site_id;
    voltdb::VoltDBEngine *engine;
    std::string catalog_string;
    // Not the catalog that the VoltDBEngine uses; a duplicate made
    // locally to get GUIDs
    catalog::Catalog *catalog;
    catalog::Cluster *cluster;
    catalog::Database *database;
    catalog::Constraint *constraint;

    voltdb::Table* warehouse_table;
    voltdb::Table* stock_table;

    int warehouse_table_id;
    int stock_table_id;
    voltdb::CatalogId xact_id;

    voltdb::CatalogId table_id;
    voltdb::Table* table; // persistent table with indexes
    std::string table_name;

    //
    // Empty placeholder objects
    //
    voltdb::NValueArray empty_params;

    bool init() {
        //
        // Create the columns for our main table
        //
        std::string *columnNames = new std::string[NUM_OF_COLUMNS];
        std::vector<voltdb::ValueType> columnTypes;
        std::vector<uint16_t> columnLengths;
        std::vector<bool> columnAllowNull;
        std::vector<boost::shared_ptr<const voltdb::TableColumn> > table_columns;
        char buffer[32];
        for (int ctr = 0; ctr < NUM_OF_COLUMNS; ctr++) {
            ::sprintf(buffer, "column%02d", ctr);
            columnNames[ctr] = buffer;
            columnTypes.push_back(COLUMN_TYPES[ctr]);
            columnLengths.push_back(COLUMN_SIZES[ctr]);
            columnAllowNull.push_back(COLUMN_ALLOW_NULLS[ctr]);
        }
        voltdb::TupleSchema *schema =
          voltdb::TupleSchema::createTupleSchema(columnTypes, columnLengths, columnAllowNull, true);

        //
        // Initialize the indexes for the main table
        //
        voltdb::TableIndexScheme pkey_scheme;
        std::vector<voltdb::TableIndexScheme> index_schemes;
        for (int ctr = 0; ctr < NUM_OF_INDEXES; ctr++) {
            std::vector<int> index_columns;
            std::vector<voltdb::ValueType> column_types;
            for (int col_ctr = 0; col_ctr < NUM_OF_COLUMNS; col_ctr++) {
                //
                // Include the column at this position in this index if it was defined to be true above
                //
                if (INDEX_COLUMNS[ctr][col_ctr]) {
                    index_columns.push_back(col_ctr);
                    column_types.push_back(voltdb::VALUE_TYPE_BIGINT);
                }
            }
            ::sprintf(buffer, "index%02d", ctr);
            voltdb::TableIndexScheme schemata(buffer, voltdb::BALANCED_TREE_INDEX,
                                              index_columns, column_types,
                                              (ctr == INDEX_PRIMARY_KEY), true, schema);
            if (ctr == INDEX_PRIMARY_KEY) {
                pkey_scheme = schemata;
            } else {
                index_schemes.push_back(schemata);
            }
        }
        voltdb::VoltDBEngine engine;
        engine.initialize(0,0);
        this->table =
          voltdb::TableFactory::getPersistentTable(this->database_id,
                                                   1, engine.getExecutorContext(), table_name,
                                                   schema, columnNames,
                                                   pkey_scheme, index_schemes, -1, false, false);

        // clean up
        delete[] columnNames;

        //
        // Fill in tuples such that we can actually iterate over indexes
        //
        int64_t second_value = 0;
        for (int64_t tuple_ctr = 0; tuple_ctr < NUM_OF_TUPLES; tuple_ctr++) {
            voltdb::TableTuple tuple = this->table->tempTuple();
            //
            // The first value needs to be unique
            // And then the second value we want to increment in intervals
            //
            tuple.setNValue(0, ValueFactory::getBigIntValue(tuple_ctr));
            tuple.setNValue(1, ValueFactory::getBigIntValue(tuple_ctr % 10 == 0 ? ++second_value : second_value));
            //
            // Then just add random values for the rest...
            //
            for (int col_ctr = 2; col_ctr < NUM_OF_COLUMNS; col_ctr++) {
                tuple.setNValue(col_ctr, getRandomValue(COLUMN_TYPES[col_ctr]));
            }
            EXPECT_EQ(true, this->table->insertTuple(tuple));
        }
        return true;
    }
};

// ---------------------------------------------------------------
// IndexScan
// ---------------------------------------------------------------
TEST_F(ExecutionPlanNodeTest, IndexScan) {
    // ::printf("table:%s\n", table->debug().c_str());
    //::srand(0);
    int tuple_ctr = 0;

    //
    // Go through each index in our main table and construct an IndexScanPlanNode to test
    // that we can successfully lookup each tuple in the table.
    //
    std::vector<voltdb::TableIndex*> indexes = this->table->allIndexes();
    ASSERT_EQ(NUM_OF_INDEXES, indexes.size());
    for (int idx_ctr = 0; idx_ctr < NUM_OF_INDEXES; idx_ctr++) {
        //
        // IndexScans have to be given a vector of ParameterValueExpression that will be used to generate
        // the search key against the index...
        // Therefore we will just grab the right values needed by the index and use that as the input params
        //
        voltdb::TableIndex* index = indexes[idx_ctr];
        //std::vector<boost::shared_ptr<const voltdb::TableColumn> > columns;
        std::vector<int> col_indexes = index->getColumnIndices();
        /*for (size_t col_ctr = 0, col_cnt = col_indexes.size(); col_ctr < col_cnt; col_ctr++) {
            columns.push_back(this->table->getColumn(col_indexes[col_ctr]));
        }*/

        //
        // Set up search key for index scan
        // Pick a random tuple from the input table to use. This guarentees
        // that we should always be getting at least one matching tuple
        //
        voltdb::NValueArray input_params((int)index->getColumnIndices().size());
        std::vector<voltdb::AbstractExpression* > searchkey_exps;
        voltdb::TableTuple tuple(table->schema());
        tableutil::getRandomTuple(table, tuple);

        for (int col_ctr = 0, col_cnt = (int)index->getColumnIndices().size(); col_ctr < col_cnt; col_ctr++) {
            int col_index = index->getColumnIndices()[col_ctr];
            input_params[col_ctr] = tuple.getNValue(col_index);
            // boost::shared_ptr<voltdb::AbstractExpression> exp = voltdb::ParameterValueExpression::getInstance(col_ctr);
            // exp->setValueType(COLUMN_TYPES[col_index]);
            voltdb::AbstractExpression *exp = voltdb::parameterValueFactory(col_ctr);
            searchkey_exps.push_back(exp);
        }
        ASSERT_EQ(true, (searchkey_exps.size() > 0));

        //
        // Construct an IndexScan node
        //
        voltdb::IndexScanPlanNode* is_node = new voltdb::IndexScanPlanNode(voltdb::AbstractPlanNode::getNextPlanNodeId());
        is_node->setTargetTable(this->table);
        is_node->setTargetTableName(this->table->name());
        is_node->setTargetIndexName(index->getName());
        is_node->setSearchKeyExpressions(searchkey_exps);

        //printf("%s\n", this->table->debug().c_str());
        //printf("%s\n", input_table->debug().c_str());
        //printf("%s\n", index->debug().c_str());
        int mem = 0;
        voltdb::IndexScanExecutor executor(this->engine, is_node);
        ASSERT_EQ(true, executor.init(engine, NULL, &mem));
        ASSERT_EQ(true, executor.execute(input_params));

        //
        // Make sure that the executor made our table
        //
        voltdb::Table* output_table = is_node->getOutputTable();
        ASSERT_EQ(true, (output_table != NULL));

        //printf("%s\n", output_table->debug().c_str());
        //exit(1);

        //
        // Now loop through our output table and make sure that the columns used in the index
        // match exactly with what is used in the search key. Because we always pick a tuple
        // from our input table, we are sure that there will always be at least one match
        //
        ASSERT_EQ(true, (output_table->activeTupleCount() > 0));
        voltdb::TableIterator temp_iter = output_table->tableIterator();
        voltdb::TableTuple tuple0(output_table->schema());
        while (temp_iter.next(tuple0)) {
            tuple_ctr++;
            for (int param_ctr = 0, param_cnt = input_params.size(); param_ctr < param_cnt; param_ctr++) {
                int col_index = index->getColumnIndices()[param_ctr];
                EXPECT_TRUE(tuple.getNValue(col_index).op_equals(input_params[param_ctr]).isTrue());
            }
        }
        //printf("%d, %d\n", (idx_ctr + 1) * NUM_OF_TUPLES, tuple_ctr);
        //EXPECT_EQ((idx_ctr + 1) * NUM_OF_TUPLES, tuple_ctr);
        delete is_node;
    }

    //printf("%s\n", this->temp_table->debug().c_str());
}

int main() {
    return TestSuite::globalInstance()->runAll();
}

/*
// I deleted a ton of commented-out, broken tests, leaving just the
// descriptions of what was supposed to be tested.  --izzy 2009-11-13

// ------------------------------------------------------------------
// OrderBy
// ------------------------------------------------------------------

// ------------------------------------------------------------------
// OrderByLimit
// ------------------------------------------------------------------

// ------------------------------------------------------------------
// Distinct
// ------------------------------------------------------------------

// ------------------------------------------------------------------
// SeqScan
// ------------------------------------------------------------------
    //
    // Construct a SeqScan node without any predicates and then make sure that
    // the temp table matches the full table.

// ------------------------------------------------------------------
// SeqScanLimit
// ------------------------------------------------------------------
    //
    // Construct a SeqScan node without any predicates and then make sure that
    // the temp table matches the full table up to the limit number of tuples

    //
    // Inline Limit Node
    //

// ------------------------------------------------------------------
// SeqScanProjection
// ------------------------------------------------------------------
    //
    // Inline Projection Node
    // SELECT colum03, column02 FROM table WHERE column03 < column02
    // Construct a SeqScan with a filter Predicate AND a nested Projection

// ------------------------------------------------------------------
// IndexScanProjection
// ------------------------------------------------------------------
    // Inline Projection Node
    // SELECT colum02, column01 FROM table WHERE column00 = ? AND column02 < column01

// ------------------------------------------------------------------
// IndexScanAggregate
// ------------------------------------------------------------------
    //
    // Inline Aggregate Node
    //      SELECT column01 FROM table ORDER BY column01 LIMIT 1
    // This gets translated into:
    //      SELECT MAX(column01) FROM table

// ------------------------------------------------------------------
// IndexScanDistinct
// ------------------------------------------------------------------

// ------------------------------------------------------
// Union
// ------------------------------------------------------

// ------------------------------------------------------
// NestLoop
// ------------------------------------------------------
    // Create a simple join predicate:
    //      WHERE table0.col0 = table1.col0

// ---------------------------------------------------------------
// NestLoopIndex
// ---------------------------------------------------------------

// ------------------------------------------------------------------
// Insert
// ------------------------------------------------------------------

// ------------------------------------------------------------------
// DeleteSingleSite
// ------------------------------------------------------------------

// ------------------------------------------------------
// Materialize
// ------------------------------------------------------

// ------------------------------------------------------
// Materialize with string
// ------------------------------------------------------

// ------------------------------------------------------------------
// Projection (very simple)
// ------------------------------------------------------------------

// ------------------------------------------------------------------
// Projection (a bit complicated)
// ------------------------------------------------------------------
    //
    // Projection PlanNode
    // SELECT (column01+column02) as 01_plus_02, (column00*column03) as 00_times_03
// ------------------------------------------------------------------
// Update
// ------------------------------------------------------------------

//
// Aggregate
//

// ------------------------------------------------------------------
// Limit
// ------------------------------------------------------------------

// ------------------------------------------------------------------
// LimitOffset
// ------------------------------------------------------------------

///
/// Multicolumn group-by, multicolumn aggregate
///

///
/// Multicolumn group-by, multicolumn hashaggregate
///
*/
