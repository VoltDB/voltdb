/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <unistd.h>

#include "boost/format.hpp"

#include "s2geo/s2cellid.h"

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

#include "harness.h"
#include "polygons.hpp"

using std::unique_ptr;
using std::chrono::microseconds;

namespace voltdb {

class CoveringCellIndexTest : public Test {
public:

    CoveringCellIndexTest() {
        std::srand(888);
        assert (voltdb::ExecutorContext::getExecutorContext() == NULL);

        m_testPool.reset(new voltdb::Pool());
        voltdb::UndoQuantum* wantNoQuantum = NULL;
        voltdb::Topend* topless = NULL;
        m_drStream.reset(new voltdb::DRTupleStream(0, 1024));
        m_executorContext.reset(new voltdb::ExecutorContext(0,                // siteId
                                                            0,                // partitionId
                                                            wantNoQuantum,    // undoQuantum
                                                            topless,          // topend
                                                            m_testPool.get(), // tempStringPool
                                                            NULL,             // engine
                                                            "",               // hostname
                                                            0,                // hostId
                                                            m_drStream.get(), // drTupleStream
                                                            NULL,             // drReplicatedStream
                                                            0));              // drClusterId
    }

protected:
    // The tables used in this suite all have:
    // - An integer primary key on the 0th field
    // - A geography column in the 1st field
    // - Optional VARBINARY(63) columns to take up space (to test compaction)
    static const int PK_COL_INDEX = 0;
    static const int GEOG_COL_INDEX = 1;
    static const int FIRST_EXTRA_COL_INDEX = 2;

    // Create a table with the schema described above, where the
    // caller may have specified a number of extra columns.  Also add
    // two indexes: one integer primary key and one geospatial.
    static unique_ptr<PersistentTable> createTable(int numExtraCols = 0) {
        TupleSchema* schema = createTupleSchemaWithExtraCols(numExtraCols);
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
#ifndef MEMCHECK
        int rowLimit = -1;
#else
        int rowLimit = 50;
#endif

        std::cout << "\n            Loading polygons...\n";
        std::istringstream instream(POLYGONS); // defined in polygons.hpp

        TableTuple tempTuple = table->tempTuple();
        auto start = std::chrono::high_resolution_clock::now();
        std::chrono::microseconds usSpentInserting = std::chrono::duration_cast<microseconds>(start - start);

        int pk = 0;
        std::string line;
        while (std::getline(instream, line)) {
            tempTuple.setNValue(PK_COL_INDEX, ValueFactory::getIntegerValue(pk));
            tempTuple.setNValue(GEOG_COL_INDEX, polygonWktToNval(line));

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
        tempTuple.setNValue(PK_COL_INDEX, ValueFactory::getIntegerValue(pk));
        tempTuple.setNValue(GEOG_COL_INDEX, NValue::getNullValue(ValueType::tGEOGRAPHY));
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
        int numDeleted = 0;

        StandAloneTupleStorage tableTuple(table->schema());
        tableTuple.tuple().setNValue(GEOG_COL_INDEX, NValue::getNullValue(ValueType::tGEOGRAPHY));

        auto start = std::chrono::high_resolution_clock::now();
        std::chrono::microseconds usSpentDeleting = std::chrono::duration_cast<microseconds>(start - start);

        // Choose a random row, and delete it, and do this until we've
        // deleted as many rows as the caller has requested.
        // Sometimes the random number generator will select a
        // previously deleted row, and we just try again when this
        // happens.  This might seem like it would take a long time,
        // but practically it happens instantaneously.
        while (numDeleted < numTuplesToDelete) {
            int idOfTupleToDelete = std::rand() % totalTuples;
            tableTuple.tuple().setNValue(PK_COL_INDEX, ValueFactory::getIntegerValue(idOfTupleToDelete));
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

        CoveringCellIndex* ccIndex = static_cast<CoveringCellIndex*>(table->index("poly_idx"));
        TableTuple tempTuple = table->tempTuple();
        StandAloneTupleStorage searchKey(ccIndex->getKeySchema());

        int numContainingCells = 0;
        int numContainingPolygons = 0;

        for (int i = 0; i < numScans; ++i) {
            // Pick a tuple at random.
            int pk = std::rand() % numTuples;
            tempTuple.setNValue(PK_COL_INDEX, ValueFactory::getIntegerValue(pk));
            TableTuple sampleTuple = table->lookupTupleByValues(tempTuple);
            ASSERT_FALSE(sampleTuple.isNullTuple());

            NValue geog = sampleTuple.getNValue(GEOG_COL_INDEX);
            if (geog.isNull()) {
                // There is one null row in the table.
                continue;
            }

            // The centroid will be inside polygons with one ring, and
            // not inside polygons with two rings (because the second
            // ring is a hole in the center).
            NValue centroid = geog.callUnary<FUNC_VOLT_POLYGON_CENTROID>();
            int32_t numInteriorRings = ValuePeeker::peekAsBigInt(geog.callUnary<FUNC_VOLT_POLYGON_NUM_INTERIOR_RINGS>());

            bool isValid = ValuePeeker::peekBoolean(geog.callUnary<FUNC_VOLT_IS_VALID_POLYGON>());
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

                    int foundPk = ValuePeeker::peekAsInteger(foundTuple.getNValue(PK_COL_INDEX));
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
        if (expectedTuples.empty()) {
            EXPECT_FALSE(b);
            return;
        }

        EXPECT_TRUE(b);

        std::set<int32_t> foundTuples;
        TableTuple foundTuple(table->schema());
        foundTuple = ccIndex->nextValueAtKey(cursor);
        while (! foundTuple.isNullTuple()) {

            int pk = ValuePeeker::peekAsInteger(foundTuple.getNValue(PK_COL_INDEX));
            foundTuples.insert(pk);

            foundTuple = ccIndex->nextValueAtKey(cursor);
        }

        EXPECT_EQ(expectedTuples.size(), foundTuples.size());

        BOOST_FOREACH(int32_t expectedPk, expectedTuples) {
            EXPECT_TRUE(foundTuples.find(expectedPk) != foundTuples.end());
        }
    }

    static NValue polygonWktToNval(const std::string& wkt) {
        NValue input = ValueFactory::getTempStringValue(wkt);
        NValue result = input.callUnary<FUNC_VOLT_POLYGONFROMTEXT>();
        return result;
    }

    static NValue pointWktToNval(const std::string& wkt) {
        NValue input = ValueFactory::getTempStringValue(wkt);
        NValue result = input.callUnary<FUNC_VOLT_POINTFROMTEXT>();
        return result;
    }

    std::string nvalToWkt(const NValue& nval) {
        ValueType vt = ValuePeeker::peekValueType(nval);
        NValue wkt;
        switch (vt) {
            case ValueType::tGEOGRAPHY:
            wkt = nval.callUnary<FUNC_VOLT_ASTEXT_GEOGRAPHY>();
            break;
            case ValueType::tPOINT:
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

private:
    // Create a tuple schema where the first two columns are
    //   INTEGER
    //   GEOGRAPHY(32767)
    // And the rest are VARBINARY(63)
    static TupleSchema* createTupleSchemaWithExtraCols(int numExtraCols) {
        TupleSchemaBuilder builder(2 + numExtraCols);
        builder.setColumnAtIndex(PK_COL_INDEX, ValueType::tINTEGER);
        builder.setColumnAtIndex(GEOG_COL_INDEX, ValueType::tGEOGRAPHY, 32767);
        for (int i = FIRST_EXTRA_COL_INDEX; i < 2 + numExtraCols; ++i) {
            builder.setColumnAtIndex(i, ValueType::tVARBINARY, UNINLINEABLE_OBJECT_LENGTH - 1);
        }
        return builder.build();
    }

    static TableIndex* createPrimaryKeyIndex(const TupleSchema* schema) {
        std::vector<int32_t> columnIndices;
        // Note: the static_cast on the following line allows us to
        // define PK_COL_INDEX as a static constant inside the class
        // definition.  Without the cast, there are linker errors.
        columnIndices.push_back(static_cast<int32_t>(PK_COL_INDEX));
        std::vector<AbstractExpression*> exprs;
        TableIndexScheme scheme("pk",
                                BALANCED_TREE_INDEX,
                                columnIndices,
                                exprs,
                                NULL,  // predicate
                                true, // unique
                                false, // countable
                                false, // migrating
                                "",    // expression as text
                                "",    // predicate as text
                                schema);
        return TableIndexFactory::getInstance(scheme);
    }

    static CoveringCellIndex* createGeospatialIndex(const TupleSchema* schema) {
        std::vector<int32_t> columnIndices;
        // Note: the static_cast on the following line allows us to
        // define GEOG_COL_INDEX as a static constant inside the class
        // definition.  Without the cast, there are linker errors.
        columnIndices.push_back(static_cast<int32_t>(GEOG_COL_INDEX));
        std::vector<AbstractExpression*> exprs;

        TableIndexScheme scheme("poly_idx",
                                COVERING_CELL_INDEX,
                                columnIndices,
                                exprs,
                                NULL,  // predicate
                                false, // unique
                                false, // countable
                                false, // migrating
                                "",    // expression as text
                                "",    // predicate as text
                                schema);
        TableIndex* index = TableIndexFactory::getInstance(scheme);
        return static_cast<CoveringCellIndex*>(index);
    }

    boost::scoped_ptr<voltdb::Pool> m_testPool;
    boost::scoped_ptr<voltdb::ExecutorContext> m_executorContext;
    boost::scoped_ptr<voltdb::AbstractDRTupleStream> m_drStream;
};

// Test table compaction, since this forces the index to be updated
// when tuples move around.
TEST_F(CoveringCellIndexTest, TableCompaction) {
    // Create a table with 120 extra cols inline so it has more than one block.
    unique_ptr<PersistentTable> table = createTable(120);
    CoveringCellIndex* ccIndex = static_cast<CoveringCellIndex*>(table->index("poly_idx"));

    loadTable(table.get());

    // Delete 99% of the records.  This should make compaction possible.
    int numTuples = table->visibleTupleCount();
    deleteSomeRecords(table.get(), numTuples, numTuples * 0.99);

#ifndef MEMCHECK
    ASSERT_TRUE(table->doForcedCompaction());
#else
        // This returns a boolean indicating if compaction was done.
        // MEMCHECK mode limits table blocks to one tuple for block, so
        // compaction won't occur.  Too bad.
        ASSERT_FALSE(table->doForcedCompaction());
#endif

    std::string msg;
    ASSERT_TRUE_WITH_MESSAGE(ccIndex->checkValidityForTest(table.get(), &msg), msg.c_str());

    std::cout << "            ";
}

// Test a larger workload of 1000 polygons.
TEST_F(CoveringCellIndexTest, LargerWorkload) {
    unique_ptr<PersistentTable> table = createTable();

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
    unique_ptr<PersistentTable> table = createTable();
    CoveringCellIndex* ccIndex = static_cast<CoveringCellIndex*>(table->index("poly_idx"));
    TableTuple tempTuple = table->tempTuple();

    tempTuple.setNValue(PK_COL_INDEX, ValueFactory::getIntegerValue(0));
    tempTuple.setNValue(GEOG_COL_INDEX, polygonWktToNval("polygon((0 0, 1 0, 0 1, 0 0))"));
    table->insertTuple(tempTuple);

    tempTuple.setNValue(PK_COL_INDEX, ValueFactory::getIntegerValue(1));
    tempTuple.setNValue(GEOG_COL_INDEX, polygonWktToNval("polygon((10 10, 11 10, 10 11, 10 10))"));
    table->insertTuple(tempTuple);

    tempTuple.setNValue(PK_COL_INDEX, ValueFactory::getIntegerValue(2));
    tempTuple.setNValue(GEOG_COL_INDEX, NValue::getNullValue(ValueType::tGEOGRAPHY));

    tempTuple.setNValue(PK_COL_INDEX, ValueFactory::getIntegerValue(3));
    tempTuple.setNValue(GEOG_COL_INDEX, polygonWktToNval("polygon((0 0, 5 0, 0 5, 0 0))"));
    table->insertTuple(tempTuple);

#ifdef VOLT_POOL_CHECKING
    ASSERT_EQ(ccIndex->getMemoryEstimate(), 1600000);
#else
    // This number is always 1440000, regardless of number of indexed
    // polygons... suspicious.  Maybe it's only considering block
    // allocations?
    ASSERT_EQ(ccIndex->getMemoryEstimate(), 1440000);
#endif

    // The size of the index in terms of indexed polygons.
    ASSERT_EQ(ccIndex->getSize(), 3);

    StandAloneTupleStorage searchKey(ccIndex->getKeySchema());
    searchKey.tuple().setNValue(0, pointWktToNval("point(0.01 0.01)"));

    scanIndexWithExpectedValues(table.get(), ccIndex, searchKey.tuple(), {0, 3});

    std::set<int32_t> emptySet;
    searchKey.tuple().setNValue(0, pointWktToNval("point(-1 -1)"));
    scanIndexWithExpectedValues(table.get(), ccIndex, searchKey.tuple(), emptySet);

    // Now try to delete a tuple.
    tempTuple.setNValue(PK_COL_INDEX, ValueFactory::getIntegerValue(3));
    tempTuple.setNValue(GEOG_COL_INDEX, NValue::getNullValue(ValueType::tGEOGRAPHY));
    TableTuple foundTuple = table->lookupTupleByValues(tempTuple);
    ASSERT_FALSE(foundTuple.isNullTuple());
    table->deleteTuple(foundTuple);

    // Verify deleted table is gone from index
    searchKey.tuple().setNValue(0, pointWktToNval("point(0.01 0.01)"));
    scanIndexWithExpectedValues(table.get(), ccIndex, searchKey.tuple(), {0});

    tempTuple.setNValue(PK_COL_INDEX, ValueFactory::getIntegerValue(0));
    foundTuple = table->lookupTupleByValues(tempTuple);
    ASSERT_FALSE(foundTuple.isNullTuple());

    tempTuple.setNValue(GEOG_COL_INDEX, polygonWktToNval("polygon((10 10, 11 10, 10 11, 10 10))"));
    table->updateTupleWithSpecificIndexes(foundTuple, tempTuple, {ccIndex});

    // Now tuple 0 should not contain this point.
    searchKey.tuple().setNValue(0, pointWktToNval("point(0.01 0.01)"));
    scanIndexWithExpectedValues(table.get(), ccIndex, searchKey.tuple(), emptySet);

    // But tuple 0 should contains this one.
    searchKey.tuple().setNValue(0, pointWktToNval("point(10.01 10.01)"));
    scanIndexWithExpectedValues(table.get(), ccIndex, searchKey.tuple(), {0, 1});

    // Searching for the null value should return nothing.
    searchKey.tuple().setNValue(0, NValue::getNullValue(ValueType::tPOINT));
    scanIndexWithExpectedValues(table.get(), ccIndex, searchKey.tuple(), emptySet);

    // Make sure the index is still valid what with all these changes and all.
    std::string msg;
    ASSERT_TRUE_WITH_MESSAGE(ccIndex->checkValidityForTest(table.get(), &msg), msg.c_str());
}

// Test the checkForIndexChange method
TEST_F(CoveringCellIndexTest, CheckForIndexChange) {
    unique_ptr<PersistentTable> table = createTable();
    CoveringCellIndex* ccIndex = static_cast<CoveringCellIndex*>(table->index("poly_idx"));

    StandAloneTupleStorage oldTuple(table->schema());
    StandAloneTupleStorage newTuple(table->schema());

    oldTuple.tuple().setNValue(PK_COL_INDEX, ValueFactory::getIntegerValue(0));
    newTuple.tuple().setNValue(PK_COL_INDEX, ValueFactory::getIntegerValue(0));

    oldTuple.tuple().setNValue(GEOG_COL_INDEX, NValue::getNullValue(ValueType::tGEOGRAPHY));
    newTuple.tuple().setNValue(GEOG_COL_INDEX, NValue::getNullValue(ValueType::tGEOGRAPHY));

    // Both tuples are null, so no index update necessary
    EXPECT_FALSE(ccIndex->checkForIndexChange(&oldTuple.tuple(), &newTuple.tuple()));

    NValue geog1 = polygonWktToNval("polygon((10 10, 11 10, 10 11, 10 10))");
    newTuple.tuple().setNValue(GEOG_COL_INDEX, geog1);

    // new tuple now non-null, index change is required.
    EXPECT_TRUE(ccIndex->checkForIndexChange(&oldTuple.tuple(), &newTuple.tuple()));

    NValue geog2 = polygonWktToNval("polygon((20 20, 21 20, 20 21, 20 20))");
    oldTuple.tuple().setNValue(GEOG_COL_INDEX, geog2);

    // Old tuple now non-null, but is a different polygon.  Index change still required.
    EXPECT_TRUE(ccIndex->checkForIndexChange(&oldTuple.tuple(), &newTuple.tuple()));

    NValue sameAsGeog1 = polygonWktToNval("polygon((10 10, 11 10, 10 11, 10 10))");
    oldTuple.tuple().setNValue(GEOG_COL_INDEX, sameAsGeog1);

    // Old tuple and new have the same polygon, but they are different instances.
    // We don't actually check to see if the polygons are the same (could be costly,
    // when most of the time the change may be required anyway).
    // Index change required.
    EXPECT_TRUE(ccIndex->checkForIndexChange(&oldTuple.tuple(), &newTuple.tuple()));

    oldTuple.tuple().setNValue(GEOG_COL_INDEX, geog1);

    // old and new tuple now contain same instance of geography, no update required.
    EXPECT_FALSE(ccIndex->checkForIndexChange(&oldTuple.tuple(), &newTuple.tuple()));
}

// Verify that unsupported methods throw fatal exceptions
TEST_F(CoveringCellIndexTest, UnsupportedMethods) {
    unique_ptr<PersistentTable> table = createTable();
    CoveringCellIndex* ccIndex = static_cast<CoveringCellIndex*>(table->index("poly_idx"));
    IndexCursor cursor(ccIndex->getTupleSchema());

    ASSERT_FATAL_EXCEPTION("unsupported on geospatial indexes", ccIndex->moveToKey(NULL, cursor));
    ASSERT_FATAL_EXCEPTION("unsupported on geospatial indexes", ccIndex->moveToKeyByTuple(NULL, cursor));
    ASSERT_FATAL_EXCEPTION("unsupported on geospatial indexes", ccIndex->hasKey(NULL));
    ASSERT_FATAL_EXCEPTION("unsupported on geospatial indexes", ccIndex->exists(NULL));
}

TEST_F(CoveringCellIndexTest, GenerateCellLevelInfo) {
    std::cout << "\n";
    for (int i = 0; i <= S2::kMaxCellLevel; ++i) {
        double areaSqM = S2Cell::AverageArea(i) * 6371008.8 * 6371008.8;

        std::cout << "    //    avg area of cells in level "
                  << boost::format("%2d: ") % i;

        if (areaSqM > 100000.0) {
            double areaSqKm = areaSqM / 1000000.0;
            std::cout << boost::format("%11.2f") % areaSqKm << " km^2\n";
        }
        else if (areaSqM > 0.1) {
            std::cout << boost::format("%11.2f") % areaSqM << " m^2\n";
        }
        else {
            double areaSqCm = areaSqM * 10000.0;
            std::cout << boost::format("%11.2f") % areaSqCm << " cm^2\n";
        }
    }

    std::cout << "            ";
}


} // end namespace voltdb

int main()
{
    return TestSuite::globalInstance()->runAll();
}
