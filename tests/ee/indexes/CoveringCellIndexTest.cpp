/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

#include <unistd.h>
#include <chrono>
#include <iostream>
#include <fstream>
#include <memory>
#include <random>

#include "harness.h"

#include "test_utils/ScopedTupleSchema.hpp"

#include "common/NValue.hpp"
#include "common/TupleSchemaBuilder.h"
#include "common/ValueFactory.hpp"
#include "common/common.h"
#include "common/tabletuple.h"
#include "expressions/functionexpression.h"
#include "indexes/CompactingTreeMultiMapIndex.h"
#include "indexes/CoveringCellIndex.h"
#include "indexes/indexkey.h"
#include "indexes/tableindex.h"
#include "indexes/tableindexfactory.h"
#include "storage/tablefactory.h"
#include "storage/persistenttable.h"

#include "polygons.hpp"

using std::unique_ptr;
using std::chrono::microseconds;

namespace voltdb {

class CoveringCellIndexTest : public Test {
public:

    static bool isMemcheck() {
#ifdef MEMCHECK
        return true;
#else
        return false;
#endif
    }

    // Create a tuple schema where the first two columns are
    //   INTEGER
    //   GEOGRAPHY(32767)
    // And the rest are VARBINARY(63)
    static TupleSchema* createTupleSchemaWithExtraCols(int numExtraCols) {
        TupleSchemaBuilder builder(2 + numExtraCols);
        builder.setColumnAtIndex(0, VALUE_TYPE_INTEGER);
        builder.setColumnAtIndex(1, VALUE_TYPE_GEOGRAPHY, 32767);
        for (int i = 2; i < 2 + numExtraCols; ++i) {
            builder.setColumnAtIndex(i, VALUE_TYPE_VARBINARY, UNINLINEABLE_OBJECT_LENGTH - 1);
        }
        return builder.build();
    }

    // Create a tuple schema where the first two columns are
    //   INTEGER
    //   GEOGRAPHY(32767)
    static TupleSchema* createTupleSchema() {
        return createTupleSchemaWithExtraCols(0);
    }

    // Create a table with the schema described above.  Also add two
    // indexes: one integer primary key and one geospatial.
    static unique_ptr<PersistentTable> createTable(TupleSchema* schema) {
        char signature[20];
        CatalogId databaseId = 1000;
        std::vector<std::string> columnNames;
        for (int i = 0; i < schema->columnCount(); ++i) {
            std::ostringstream oss;
            oss << "col_" << i;
            columnNames.push_back(oss.str());
        }
        auto table = unique_ptr<PersistentTable>(
                         static_cast<PersistentTable*>(TableFactory::getPersistentTable(databaseId,
                                                                                        "test_table",
                                                                                        schema,
                                                                                        columnNames,
                                                                                        signature)));
        table->addIndex(createGeospatialIndex(table->schema()));

        TableIndex* pkIndex = createPrimaryKeyIndex(table->schema());
        table->addIndex(pkIndex);
        table->setPrimaryKeyIndex(pkIndex);

        return table;
    }

    static TableIndex* createPrimaryKeyIndex(const TupleSchema* schema) {
        std::vector<int32_t> columnIndices;
        columnIndices.push_back(0);
        std::vector<AbstractExpression*> exprs;
        TableIndexScheme scheme("pk",
                                BALANCED_TREE_INDEX,
                                columnIndices,
                                exprs,
                                NULL,  // predicate
                                true, // unique
                                false, // countable
                                "",    // expression as text
                                "",    // predicate as text
                                schema);
        return TableIndexFactory::getInstance(scheme);
    }

    static CoveringCellIndex* createGeospatialIndex(const TupleSchema* schema) {
        std::vector<int32_t> columnIndices;
        columnIndices.push_back(1);
        std::vector<AbstractExpression*> exprs;

        TableIndexScheme scheme("poly_idx",
                                COVERING_CELL_INDEX,
                                columnIndices,
                                exprs,
                                NULL,  // predicate
                                false, // unique
                                false, // countable
                                "",    // expression as text
                                "",    // predicate as text
                                schema);
        TableIndex* index = TableIndexFactory::getInstance(scheme);
        return static_cast<CoveringCellIndex*>(index);
    }

    // Load table from the polygons in the string POLYGONS, defined in
    // polygons.hpp.  Also print out some stats about how long it
    // took.
    //
    // The workload is 1000 generated polygons created by
    // PolygonFactory in Java.  They are all bounded to an area
    // approximately in the continental US, and so may overlap:
    //   o 25% regular convex
    //   o 25% regular convex with a hole in the center
    //   o 25% star-shaped
    //   o 25% star-shaped with a hole in the center
    // Also, add a null polygon.
    //
    // In memcheck mode, only loads 50 rows.
    void loadTable(PersistentTable* table) {
        int rowLimit = -1;
        if (isMemcheck()) {
            // memcheck slows things down a lot...
            rowLimit = 50;
        }

        std::cout << "\n            Loading polygons...\n";
        std::istringstream infile(POLYGONS); // defined in polygons.hpp

        TableTuple tempTuple = table->tempTuple();
        auto start = std::chrono::high_resolution_clock::now();
        std::chrono::microseconds usSpentInserting = std::chrono::duration_cast<microseconds>(start - start);

        int pk = 0;
        std::string line;
        while (std::getline(infile, line)) {
            tempTuple.setNValue(0, ValueFactory::getIntegerValue(pk));
            tempTuple.setNValue(1, wktToNval(line));

            start = std::chrono::high_resolution_clock::now();
            table->insertTuple(tempTuple);
            auto end = std::chrono::high_resolution_clock::now();
            usSpentInserting += std::chrono::duration_cast<microseconds>(end - start);

            ++pk;
            if (rowLimit > 0 && pk > rowLimit) {
                break;
            }
        }

        std::cout << "              Average duration of insert: " << (usSpentInserting.count() / pk) << " us\n";

        // Add a null value
        tempTuple.setNValue(0, ValueFactory::getIntegerValue(pk));
        tempTuple.setNValue(1, NValue::getNullValue(VALUE_TYPE_GEOGRAPHY));
        table->insertTuple(tempTuple);

        // Dump some stats about the index.
        CoveringCellIndex* ccIndex = static_cast<CoveringCellIndex*>(table->index("poly_idx"));
        CoveringCellIndex::StatsForTest stats = ccIndex->getStatsForTest(table);

        double cellsPerPoly = double(stats.numCells) / stats.numPolygons;
        std::cout << "              Cells per polygon: " << cellsPerPoly << "\n";

        // Use km^2, since the areas are large.
        double areaPerPoly = (stats.polygonsArea / stats.numPolygons) / 1000000.0;
        double areaPerCellCovering = (stats.cellsArea / stats.numPolygons) / 1000000.0;
        std::cout << "              Average area per polygon: " << areaPerPoly << " km^2\n";
        std::cout << "              Average area per cell covering: " << areaPerCellCovering << " km^2\n";
        std::cout << "              Cell area divided by polygon area (lower is better): "
                  <<  (areaPerCellCovering / areaPerPoly) << "\n";
    }

    // Delete some records from the table, forcing update of the geospatial index.
    static int deleteSomeRecords(PersistentTable* table, int totalTuples, int numTuplesToDelete) {
        std::cout << "            Deleting " << numTuplesToDelete << " tuples...\n";
        std::default_random_engine generator;
        std::uniform_int_distribution<int> distribution(0, totalTuples - 1);
        int numDeleted = 0;

        StandAloneTupleStorage tableTuple(table->schema());
        tableTuple.tuple().setNValue(1, NValue::getNullValue(VALUE_TYPE_GEOGRAPHY));

        auto start = std::chrono::high_resolution_clock::now();
        std::chrono::microseconds usSpentDeleting = std::chrono::duration_cast<microseconds>(start - start);

        while (numDeleted < numTuplesToDelete) {
            int idOfTupleToDelete = distribution(generator);
            tableTuple.tuple().setNValue(0, ValueFactory::getIntegerValue(idOfTupleToDelete));
            TableTuple tupleToDelete = table->lookupTupleByValues(tableTuple.tuple());
            if (! tupleToDelete.isNullTuple()) {
                start = std::chrono::high_resolution_clock::now();
                table->deleteTuple(tupleToDelete);
                auto end = std::chrono::high_resolution_clock::now();
                usSpentDeleting += std::chrono::duration_cast<microseconds>(end - start);
                ++numDeleted;
            }
        }

        std::cout << "              Average duration of delete: " << (usSpentDeleting.count() / numDeleted) << " us\n";

        return numDeleted;
    }

    // Scan some records in the table, verifying that points that are
    // supposed to be inside are, and those that are not aren't.
    // Print out some stats about how long things took.
    void scanSomeRecords(PersistentTable *table, int numTuples, int numScans) {
        std::cout << "            Scanning for containing polygons on " << numScans << " points...\n";

        auto start = std::chrono::high_resolution_clock::now();
        std::chrono::microseconds usSpentScanning = std::chrono::duration_cast<microseconds>(start - start);
        std::chrono::microseconds usSpentContainsing = std::chrono::duration_cast<microseconds>(start - start);

        std::default_random_engine generator;
        std::uniform_int_distribution<int> distribution(0, numTuples - 1);
        CoveringCellIndex* ccIndex = static_cast<CoveringCellIndex*>(table->index("poly_idx"));
        TableTuple tempTuple = table->tempTuple();
        StandAloneTupleStorage searchKey(ccIndex->getKeySchema());

        int numContainingCells = 0;
        int numContainingPolygons = 0;

        for (int i = 0; i < numScans; ++i) {
            // Pick a tuple at random.
            int pk = distribution(generator);
            tempTuple.setNValue(0, ValueFactory::getIntegerValue(pk));
            TableTuple sampleTuple = table->lookupTupleByValues(tempTuple);
            ASSERT_FALSE(sampleTuple.isNullTuple());

            NValue geog = sampleTuple.getNValue(1);
            if (geog.isNull()) {
                // There is one null row in the table.
                continue;
            }

            // The centroid will be inside polygons with one ring, and
            // not inside polygons with two rings (because the second
            // ring is a hole in the center).
            NValue centroid = geog.callUnary<FUNC_VOLT_POLYGON_CENTROID>();
            int32_t numInteriorRings = ValuePeeker::peekAsBigInt(geog.callUnary<FUNC_VOLT_POLYGON_NUM_INTERIOR_RINGS>());

            bool isValid = ValuePeeker::peekBoolean(geog.callUnary<FUNC_VOLT_VALIDATE_POLYGON>());
            if (! isValid) {
                std::ostringstream oss;
                int32_t len;
                const char* reasonChars = ValuePeeker::peekObject_withoutNull(geog.callUnary<FUNC_VOLT_POLYGON_INVALID_REASON>(), &len);
                std::string reason = std::string(reasonChars, len);
                oss << "At " << i << "th scan, expected a valid polygon at pk "
                    << pk << " but isValid says its not because \""
                    << reason << "\".  WKT:\n"
                    << nvalToWkt(geog);

                ASSERT_TRUE_WITH_MESSAGE(isValid, oss.str().c_str());
            }

            start = std::chrono::high_resolution_clock::now();

            searchKey.tuple().setNValue(0, centroid);
            IndexCursor cursor(ccIndex->getTupleSchema());

            bool foundSamplePoly = false;
            bool b = ccIndex->moveToCoveringCell(&searchKey.tuple(), cursor);
            if (b) {
                TableTuple foundTuple = ccIndex->nextValueAtKey(cursor);
                while (! foundTuple.isNullTuple()) {
                    ++numContainingCells;

                    auto startContains = std::chrono::high_resolution_clock::now();
                    bool polygonContains = ValuePeeker::peekBoolean(NValue::call<FUNC_VOLT_CONTAINS>({geog, centroid}));
                    auto endContains = std::chrono::high_resolution_clock::now();
                    usSpentContainsing += std::chrono::duration_cast<microseconds>(endContains - startContains);

                    if (polygonContains)
                        ++numContainingPolygons;

                    int foundPk = ValuePeeker::peekAsInteger(foundTuple.getNValue(0));
                    if (foundPk == pk && polygonContains) {
                        foundSamplePoly = true;
                    }

                    foundTuple = ccIndex->nextValueAtKey(cursor);
                }
            }

            auto end = std::chrono::high_resolution_clock::now();
            usSpentScanning += std::chrono::duration_cast<microseconds>(end - start);

            ASSERT_TRUE(numInteriorRings == 0 || numInteriorRings == 1);

            if (numInteriorRings == 0 && !foundSamplePoly) {
                std::ostringstream oss;
                oss << "At " << i << "th scan, expected to find centroid in polygon with primary key "
                    << pk << ", centroid WKT:\n" << nvalToWkt(centroid)
                    << "\npolygon WKT:\n" << nvalToWkt(geog);
                ASSERT_TRUE_WITH_MESSAGE(foundSamplePoly, oss.str().c_str());
            }
            else if (numInteriorRings == 1) {
                // There was a hole in the center so the centroid is not in the polygon
                ASSERT_TRUE_WITH_MESSAGE(!foundSamplePoly, "Expected to not find centroid contained by polygon with hole in the center");
            }
        }

        auto avgTotalUsSpentScanning = usSpentScanning.count() / numScans;
        auto avgUsSpentContainsing = usSpentContainsing.count() / numScans;
        auto avgUsSpentScanning = avgTotalUsSpentScanning - avgUsSpentContainsing;
        std::cout << "              Average duration of each index lookup total: " << avgTotalUsSpentScanning << " us\n";
        std::cout << "                Average duration spent on CONTAINS: " << avgUsSpentContainsing << " us\n";
        std::cout << "                Average duration spent on B-tree traversal: " << avgUsSpentScanning << " us\n";

        double pctFalsePositives = (double(numContainingCells - numContainingPolygons) / numContainingCells) * 100.0;
        std::cout << "              Percent false positives (point in cell but not polygon): " << pctFalsePositives << "%\n";

        double avgCellsContainingPoint = numContainingCells / double(numScans);
        double avgPolygonsContainingPoint = numContainingPolygons / double(numScans);
        std::cout << "                On average, each point was in " << avgCellsContainingPoint << " cells\n";
        std::cout << "                On average, each point was in " << avgPolygonsContainingPoint << " polygons\n";
    }

    // Given a table, an index, and search key (point), find the
    // polygons in the table that contain the point.  The expected
    // tuples are identified by their primary key values.
    void scanIndexWithExpectedValues(Table* table,
                                     CoveringCellIndex* ccIndex,
                                     const TableTuple& searchKey,
                                     const std::set<int32_t> &expectedTuples) {
        IndexCursor cursor(ccIndex->getTupleSchema());

        bool b = ccIndex->moveToCoveringCell(&searchKey, cursor);
        if (expectedTuples.size() > 0) {
            EXPECT_TRUE(b);
        }
        else {
            EXPECT_FALSE(b);
            return;
        }

        std::set<int32_t> foundTuples;
        TableTuple foundTuple(table->schema());
        foundTuple = ccIndex->nextValueAtKey(cursor);
        while (! foundTuple.isNullTuple()) {

            int pk = ValuePeeker::peekAsInteger(foundTuple.getNValue(0));
            foundTuples.insert(pk);

            foundTuple = ccIndex->nextValueAtKey(cursor);
        }

        EXPECT_EQ(expectedTuples.size(), foundTuples.size());

        BOOST_FOREACH(int32_t expectedPk, expectedTuples) {
            EXPECT_TRUE(foundTuples.find(expectedPk) != foundTuples.end());
        }
    }

    static NValue wktToNval(const std::string& wkt) {
        NValue result;
        NValue input = ValueFactory::getTempStringValue(wkt);
        try {
            result = input.callUnary<FUNC_VOLT_POLYGONFROMTEXT>();
        }
        catch (const SQLException &exc) {
            // It might be a point and not a polygon.
            result = input.callUnary<FUNC_VOLT_POINTFROMTEXT>();
        }

        return result;
    }

    std::string nvalToWkt(const NValue& nval) {
        ValueType vt = ValuePeeker::peekValueType(nval);
        NValue wkt;
        switch (vt) {
        case VALUE_TYPE_GEOGRAPHY:
            wkt = nval.callUnary<FUNC_VOLT_ASTEXT_GEOGRAPHY>();
            break;
        case VALUE_TYPE_POINT:
            wkt = nval.callUnary<FUNC_VOLT_ASTEXT_GEOGRAPHY_POINT>();
            break;
        default:
            wkt = ValueFactory::getTempStringValue("Something that is not a point or polygon");
        }

        if (! wkt.isNull()) {
            int32_t len;
            const char* wktChars = ValuePeeker::peekObject_withoutNull(wkt, &len);
            return std::string(wktChars, len);
        }

        return "NULL";
    }
};

// Test table compaction, since this forces the index to be updated
// when tuples move around.
TEST_F(CoveringCellIndexTest, TableCompaction) {
    // Create a table with some extra cols inline so it has more than one block.
    unique_ptr<PersistentTable> table = createTable(createTupleSchemaWithExtraCols(120));
    CoveringCellIndex* ccIndex = static_cast<CoveringCellIndex*>(table->index("poly_idx"));

    loadTable(table.get());

    // Delete 99% of the records.  This should make compaction possible.
    int numTuples = table->visibleTupleCount();
    deleteSomeRecords(table.get(), numTuples, numTuples * 0.99);

    if (isMemcheck()) {
        // This returns a boolean indicating if compaction was done.
        // MEMCHECK mode limits table blocks to one tuple for block, so
        // compaction won't occur.  Too bad.
        ASSERT_FALSE(table->doForcedCompaction());
    }
    else {
        ASSERT_TRUE(table->doForcedCompaction());
    }

    std::string msg;
    ASSERT_TRUE_WITH_MESSAGE(ccIndex->checkValidityForTest(table.get(), &msg), msg.c_str());

    std::cout << "            ";
}

// Test a larger workload of 1000 polygons.
TEST_F(CoveringCellIndexTest, LargerWorkload) {
    unique_ptr<PersistentTable> table = createTable(createTupleSchema());

    loadTable(table.get());
    CoveringCellIndex* ccIndex = static_cast<CoveringCellIndex*>(table->index("poly_idx"));

    std::string msg;
    ASSERT_TRUE_WITH_MESSAGE(ccIndex->checkValidityForTest(table.get(), &msg), msg.c_str());

    int numTuples = table->visibleTupleCount();

    scanSomeRecords(table.get(), numTuples, numTuples);

    deleteSomeRecords(table.get(), numTuples, numTuples / 10);

    EXPECT_TRUE(ccIndex->checkValidityForTest(table.get(), &msg));
    std::cout << "            ";
}

// Test basic insert, scan, update and delete operations.
TEST_F(CoveringCellIndexTest, Simple) {
    unique_ptr<PersistentTable> table = createTable(createTupleSchema());
    CoveringCellIndex* ccIndex = static_cast<CoveringCellIndex*>(table->index("poly_idx"));
    TableTuple tempTuple = table->tempTuple();

    tempTuple.setNValue(0, ValueFactory::getIntegerValue(0));
    tempTuple.setNValue(1, wktToNval("polygon((0 0, 1 0, 0 1, 0 0))"));
    table->insertTuple(tempTuple);

    tempTuple.setNValue(0, ValueFactory::getIntegerValue(1));
    tempTuple.setNValue(1, wktToNval("polygon((10 10, 11 10, 10 11, 10 10))"));
    table->insertTuple(tempTuple);

    tempTuple.setNValue(0, ValueFactory::getIntegerValue(2));
    tempTuple.setNValue(1, NValue::getNullValue(VALUE_TYPE_GEOGRAPHY));

    tempTuple.setNValue(0, ValueFactory::getIntegerValue(3));
    tempTuple.setNValue(1, wktToNval("polygon((0 0, 5 0, 0 5, 0 0))"));
    table->insertTuple(tempTuple);

    // This number is always 1440000, regardless of number of indexed
    // polygons... suspicious.  Maybe it's only considering block
    // allocations?
    ASSERT_EQ(ccIndex->getMemoryEstimate(), 1440000);

    // The size of the index in terms of indexed polygons.
    ASSERT_EQ(ccIndex->getSize(), 3);

    StandAloneTupleStorage searchKey(ccIndex->getKeySchema());
    searchKey.tuple().setNValue(0, wktToNval("point(0.01 0.01)"));

    scanIndexWithExpectedValues(table.get(), ccIndex, searchKey.tuple(), {0, 3});

    searchKey.tuple().setNValue(0, wktToNval("point(-1 -1)"));
    scanIndexWithExpectedValues(table.get(), ccIndex, searchKey.tuple(), {});

    // Now try to delete a tuple.
    tempTuple.setNValue(0, ValueFactory::getIntegerValue(3));
    tempTuple.setNValue(1, NValue::getNullValue(VALUE_TYPE_GEOGRAPHY));
    TableTuple foundTuple = table->lookupTupleByValues(tempTuple);
    ASSERT_FALSE(foundTuple.isNullTuple());
    ASSERT_TRUE(table->deleteTuple(foundTuple));

    // Verify deleted table is gone from index
    searchKey.tuple().setNValue(0, wktToNval("point(0.01 0.01)"));
    scanIndexWithExpectedValues(table.get(), ccIndex, searchKey.tuple(), {0});

    tempTuple.setNValue(0, ValueFactory::getIntegerValue(0));
    foundTuple = table->lookupTupleByValues(tempTuple);
    ASSERT_FALSE(foundTuple.isNullTuple());

    tempTuple.setNValue(1, wktToNval("polygon((10 10, 11 10, 10 11, 10 10))"));
    bool success = table->updateTupleWithSpecificIndexes(foundTuple,
                                                         tempTuple,
                                                         {ccIndex});
    ASSERT_TRUE(success);

    // Now tuple 0 should not contain this point.
    searchKey.tuple().setNValue(0, wktToNval("point(0.01 0.01)"));
    scanIndexWithExpectedValues(table.get(), ccIndex, searchKey.tuple(), {});

    // But tuple 0 should contains this one.
    searchKey.tuple().setNValue(0, wktToNval("point(10.01 10.01)"));
    scanIndexWithExpectedValues(table.get(), ccIndex, searchKey.tuple(), {0, 1});

    // Make sure the index is still valid what with all these changes and all.
    std::string msg;
    ASSERT_TRUE_WITH_MESSAGE(ccIndex->checkValidityForTest(table.get(), &msg), msg.c_str());
}

// Test the checkForIndexChange method
TEST_F(CoveringCellIndexTest, CheckForIndexChange) {
    unique_ptr<PersistentTable> table = createTable(createTupleSchema());
    CoveringCellIndex* ccIndex = static_cast<CoveringCellIndex*>(table->index("poly_idx"));

    StandAloneTupleStorage oldTuple(table->schema());
    StandAloneTupleStorage newTuple(table->schema());

    oldTuple.tuple().setNValue(0, ValueFactory::getIntegerValue(0));
    newTuple.tuple().setNValue(0, ValueFactory::getIntegerValue(0));

    oldTuple.tuple().setNValue(1, NValue::getNullValue(VALUE_TYPE_GEOGRAPHY));
    newTuple.tuple().setNValue(1, NValue::getNullValue(VALUE_TYPE_GEOGRAPHY));

    // Both tuples are null, so no index update necessary
    EXPECT_FALSE(ccIndex->checkForIndexChange(&oldTuple.tuple(), &newTuple.tuple()));

    NValue geog1 = wktToNval("polygon((10 10, 11 10, 10 11, 10 10))");
    newTuple.tuple().setNValue(1, geog1);

    // new tuple now non-null, index change is required.
    EXPECT_TRUE(ccIndex->checkForIndexChange(&oldTuple.tuple(), &newTuple.tuple()));

    NValue geog2 = wktToNval("polygon((20 20, 21 20, 20 21, 20 20))");
    oldTuple.tuple().setNValue(1, geog2);

    // Old tuple now non-null, but is a different polygon.  Index change still required.
    EXPECT_TRUE(ccIndex->checkForIndexChange(&oldTuple.tuple(), &newTuple.tuple()));

    NValue sameAsGeog1 = wktToNval("polygon((10 10, 11 10, 10 11, 10 10))");
    oldTuple.tuple().setNValue(1, sameAsGeog1);

    // Old tuple and new have the same polygon, but they are different instances.
    // We don't actually check to see if the polygons are the same (could be costly,
    // when most of the time the change may be required anyway).
    // Index change required.
    EXPECT_TRUE(ccIndex->checkForIndexChange(&oldTuple.tuple(), &newTuple.tuple()));

    oldTuple.tuple().setNValue(1, geog1);

    // old and new tuple now contain same instance of geography, no update required.
    EXPECT_FALSE(ccIndex->checkForIndexChange(&oldTuple.tuple(), &newTuple.tuple()));
}

// This ought to go into harness.h, but I experienced compile failures
// (on OS X) when I tried to put it there.  Something about a .h file
// containing C++ code.
#define ASSERT_FATAL_EXCEPTION(msgFragment, expr)                       \
    do {                                                                \
        try {                                                           \
            expr;                                                       \
            fail(__FILE__, __LINE__,                                    \
                 "expected FatalException that did not occur");         \
            EXPECT_FALSE(true);                                         \
        }                                                               \
        catch (FatalException& exc) {                                   \
            std::ostringstream oss;                                     \
            oss << "did not find \""                                    \
                << (msgFragment) << "\" in \""                          \
                << exc.m_reason << "\"";                                \
            ASSERT_TRUE_WITH_MESSAGE(exc.m_reason.find(msgFragment) != std::string::npos, \
                                     oss.str().c_str());                \
        }                                                               \
    } while(false)

// Verify that unsupported methods throw fatal exceptions
TEST_F(CoveringCellIndexTest, UnsupportedMethods) {
    unique_ptr<PersistentTable> table = createTable(createTupleSchema());
    CoveringCellIndex* ccIndex = static_cast<CoveringCellIndex*>(table->index("poly_idx"));
    IndexCursor cursor(ccIndex->getTupleSchema());

    ASSERT_FATAL_EXCEPTION("unsupported on geospatial indexes", ccIndex->moveToKey(NULL, cursor));
    ASSERT_FATAL_EXCEPTION("unsupported on geospatial indexes", ccIndex->moveToKeyByTuple(NULL, cursor));
    ASSERT_FATAL_EXCEPTION("unsupported on geospatial indexes", ccIndex->hasKey(NULL));
    ASSERT_FATAL_EXCEPTION("unsupported on geospatial indexes", ccIndex->exists(NULL));
}

} // end namespace voltdb

int main()
{
    assert (voltdb::ExecutorContext::getExecutorContext() == NULL);

    boost::scoped_ptr<voltdb::Pool> testPool(new voltdb::Pool());
    voltdb::UndoQuantum* wantNoQuantum = NULL;
    voltdb::Topend* topless = NULL;
    boost::scoped_ptr<voltdb::ExecutorContext>
        executorContext(new voltdb::ExecutorContext(0,              // siteId
                                                    0,              // partitionId
                                                    wantNoQuantum,  // undoQuantum
                                                    topless,        // topend
                                                    testPool.get(), // tempStringPool
                                                    NULL,           // params
                                                    NULL,           // engine
                                                    "",             // hostname
                                                    0,              // hostId
                                                    NULL,           // drTupleStream
                                                    NULL,           // drReplicatedStream
                                                    0));            // drClusterId

    return TestSuite::globalInstance()->runAll();
}
