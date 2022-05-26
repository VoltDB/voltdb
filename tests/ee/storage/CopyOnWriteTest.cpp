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

#include "harness.h"

#include "common/NValue.hpp"
#include "common/TupleOutputStream.h"
#include "common/TupleOutputStreamProcessor.h"
#include "common/TupleSchema.h"
#include "common/types.h"
#include "common/ValueFactory.hpp"
#include "common/ValuePeeker.hpp"
#include "execution/VoltDBEngine.h"
#include "expressions/expressions.h"
#include "indexes/tableindex.h"
#include "indexes/tableindexfactory.h"
#include "storage/CopyOnWriteIterator.h"
#include "storage/DRTupleStream.h"
#include "storage/ElasticContext.h"
#include "storage/ElasticScanner.h"
#include "storage/persistenttable.h"
#include "storage/tablefactory.h"
#include "storage/tableiterator.h"
#include "storage/TableStreamerContext.h"
#include "storage/tableutil.h"

#include <boost/foreach.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/ptr_container/ptr_vector.hpp>

#include "stx/btree_set.h"

#include <murmur3/MurmurHash3.h>

#include "json/json.h"

#include <iostream>
#include <stdint.h>
#include <stdarg.h>
#include <string>
#include <vector>


using namespace voltdb;

/**
 * Counter for unique primary key values
 */
static int32_t m_primaryKeyIndex = 0;

// Selects extra-small quantity for debugging.
//IMPORTANT: Comment out EXTRA_SMALL #define before checking in to reenable full testing!
//#define EXTRA_SMALL

#if defined(EXTRA_SMALL)

// Extra small quantities for quick debugging runs.
const size_t TUPLE_COUNT = 10;
const size_t BUFFER_SIZE = 1024;
const size_t NUM_REPETITIONS = 2;
const size_t NUM_MUTATIONS = 5;

#elif defined(MEMCHECK)

// The smaller quantity is used for memcheck runs.
const size_t TUPLE_COUNT = 1000;
const size_t BUFFER_SIZE = 131072;
const size_t NUM_REPETITIONS = 10;
const size_t NUM_MUTATIONS = 10;

#else

// Normal/full run quantities.
const size_t TUPLE_COUNT = 174762;
const size_t BUFFER_SIZE = 131072;
const size_t NUM_REPETITIONS = 10;
const size_t NUM_MUTATIONS = 10;

#endif

// Maximum quantity for detailed error display.
const size_t MAX_DETAIL_COUNT = 50;

// Handy types and values.
typedef int64_t T_Value;
typedef stx::btree_set<T_Value> T_ValueSet;

class T_HashRange : public std::pair<int32_t, int32_t> {
public:
    T_HashRange(int32_t i1, int32_t i2, bool empty = false) :
            std::pair<int32_t, int32_t>(i1, i2),
            m_empty(empty)
    {}

    bool inRange(int32_t i) {
        if (first >= second) {
            return i >= first || i < second;
        }
        return i >= first && i < second;
    }

    std::string label(const std::string &tag) {
        std::ostringstream label;
        label << tag << ' ' << first << ':' << second;
        return label.str();
    }

    bool m_empty;
};
typedef std::vector<T_HashRange> T_HashRangeVector;

/**
 * The strategy of this test is to create a table with 5 blocks of tuples with the first column (primary key)
 * sequentially numbered, serialize the whole thing to a block of memory, go COW and start serializing tuples
 * from the table while doing random updates, inserts, and deletes, then take that serialization output, sort it, and
 * then compare it to the original serialization output. They should be bit equivalent. Repeat this process another two
 * times.
 */
class CopyOnWriteTest : public Test {
public:
    CopyOnWriteTest() : m_table(NULL), drStream(0) {
        m_tuplesInserted = 0;
        m_tuplesUpdated = 0;
        m_tuplesDeleted = 0;
        m_tuplesInsertedInLastUndo = 0;
        m_tuplesDeletedInLastUndo = 0;
        m_engine = new voltdb::VoltDBEngine();
        int partitionCount = 1;
        int tokenCount = htonl(100);
        int partitionId = htonl(0);

        m_engine->initialize(1,1, 0, partitionCount, 0, "", 0, 1024, false, -1, false, DEFAULT_TEMP_TABLE_MEMORY, false);
        partitionCount = htonl(partitionCount);
        int data[3] = {partitionCount, tokenCount, partitionId};
        m_engine->updateHashinator((char*)data, NULL, 0);

        m_columnNames.push_back("1");
        m_columnNames.push_back("2");
        m_columnNames.push_back("3");
        m_columnNames.push_back("4");
        m_columnNames.push_back("5");
        m_columnNames.push_back("6");
        m_columnNames.push_back("7");
        m_columnNames.push_back("8");
        m_columnNames.push_back("9");

        m_tableSchemaTypes.push_back(voltdb::ValueType::tINTEGER);
        m_tableSchemaTypes.push_back(voltdb::ValueType::tINTEGER);

        //Filler columns
        m_tableSchemaTypes.push_back(voltdb::ValueType::tBIGINT);
        m_tableSchemaTypes.push_back(voltdb::ValueType::tBIGINT);
        m_tableSchemaTypes.push_back(voltdb::ValueType::tBIGINT);
        m_tableSchemaTypes.push_back(voltdb::ValueType::tBIGINT);
        m_tableSchemaTypes.push_back(voltdb::ValueType::tBIGINT);
        m_tableSchemaTypes.push_back(voltdb::ValueType::tBIGINT);
        m_tableSchemaTypes.push_back(voltdb::ValueType::tBIGINT);

        m_tupleWidth = (sizeof(int32_t) * 2) + (sizeof(int64_t) * 7);

        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::ValueType::tINTEGER));
        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::ValueType::tINTEGER));

        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::ValueType::tBIGINT));
        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::ValueType::tBIGINT));
        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::ValueType::tBIGINT));
        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::ValueType::tBIGINT));
        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::ValueType::tBIGINT));
        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::ValueType::tBIGINT));
        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::ValueType::tBIGINT));

        m_tableSchemaAllowNull.push_back(false);
        m_tableSchemaAllowNull.push_back(false);
        m_tableSchemaAllowNull.push_back(false);
        m_tableSchemaAllowNull.push_back(false);
        m_tableSchemaAllowNull.push_back(false);
        m_tableSchemaAllowNull.push_back(false);
        m_tableSchemaAllowNull.push_back(false);
        m_tableSchemaAllowNull.push_back(false);
        m_tableSchemaAllowNull.push_back(false);
        m_tableSchemaAllowNull.push_back(false);

        m_primaryKeyIndexColumns.push_back(0);

        m_undoToken = 0;

        m_tableId = 0;

        m_npartitions = 1;

        m_niteration = -1;

        m_nerrors = 0;

        m_showTuples = TUPLE_COUNT <= MAX_DETAIL_COUNT;

        strcpy(m_stage, "Initialize");

        ExecutorContext::getExecutorContext()->setDrStream(&drStream);
    }

    ~CopyOnWriteTest() {
        delete m_engine;
        if (m_table != NULL) {
            delete m_table;
        }
    }

    void iterate() {
        m_niteration++;
    }

    void initTable(int nparts_, int tableAllocationTargetSize) {
        m_npartitions = nparts_;
        m_tableSchema = voltdb::TupleSchema::createTupleSchemaForTest(m_tableSchemaTypes,
                                                               m_tableSchemaColumnSizes,
                                                               m_tableSchemaAllowNull);

        voltdb::TableIndexScheme indexScheme("primaryKeyIndex",
                                             voltdb::BALANCED_TREE_INDEX,
                                             m_primaryKeyIndexColumns,
                                             TableIndex::simplyIndexColumns(),
                                             true, true, false, m_tableSchema);
        std::vector<voltdb::TableIndexScheme> indexes;

        if (m_table != NULL) {
            delete m_table;
        }

        m_table = dynamic_cast<voltdb::PersistentTable*>(
                voltdb::TableFactory::getPersistentTable(m_tableId, "Foo", m_tableSchema,
                                                         m_columnNames, signature, false, 0, PERSISTENT,
                                                         tableAllocationTargetSize));

        TableIndex *pkeyIndex = TableIndexFactory::getInstance(indexScheme);
        assert(pkeyIndex);
        m_table->addIndex(pkeyIndex);
        m_table->setPrimaryKeyIndex(pkeyIndex);

        TableTuple tuple(m_table->schema());
        size_t i = 0;
        voltdb::TableIterator iterator = m_table->iterator();
        while (iterator.next(tuple)) {
            int64_t value = *reinterpret_cast<const int64_t*>(tuple.address() + 1);
            m_values.push_back(value);
            m_valueSet.insert(std::pair<int64_t,size_t>(value, i++));
        }
    }

    void addRandomUniqueTuples(Table *table, int numTuples, T_ValueSet *set = NULL) {
        TableTuple tuple = table->tempTuple();
        ::memset(tuple.address() + 1, 0, tuple.tupleLength() - 1);
        for (int ii = 0; ii < numTuples; ii++) {
            int value = rand();
            tuple.setNValue(0, ValueFactory::getIntegerValue(m_primaryKeyIndex++));
            tuple.setNValue(1, ValueFactory::getIntegerValue(value));
            bool success = table->insertTuple(tuple);
            if (!success) {
                std::cout << "Failed to add random unique tuple" << std::endl;
                return;
            }
            if (set != NULL) {
                set->insert(*reinterpret_cast<const int64_t*>(tuple.address() + 1));
            }
        }
    }

    void doRandomUndo() {
        int rand = ::rand();
        int op = rand % 2;
        switch (op) {

                /*
                 * Undo the last quantum
                 */
            case 0: {
                m_engine->undoUndoToken(m_undoToken);
                m_tuplesDeleted -= m_tuplesDeletedInLastUndo;
                m_tuplesInserted -= m_tuplesInsertedInLastUndo;
                break;
            }

                /*
                 * Release the last quantum
                 */
            case 1: {
                m_engine->releaseUndoToken(m_undoToken, false);
                break;
            }
        }
        m_engine->setUndoToken(++m_undoToken);
        ExecutorContext::getExecutorContext()->setupForPlanFragments(m_engine->getCurrentUndoQuantum(),
                                                                     0, 0, 0, 0, false);
        m_tuplesDeletedInLastUndo = 0;
        m_tuplesInsertedInLastUndo = 0;
    }

    void doRandomDelete(PersistentTable *table, T_ValueSet *set = NULL) {
        TableTuple tuple(table->schema());
        if (tableutil::getRandomTuple(table, tuple)) {
            if (set != NULL) {
                set->insert(*reinterpret_cast<const int64_t*>(tuple.address() + 1));
            }
            table->deleteTuple(tuple, true);
            m_tuplesDeleted++;
            m_tuplesDeletedInLastUndo++;
        }
    }

    void updateSpecificTuple(PersistentTable *table, voltdb::TableTuple tuple, T_ValueSet *setFrom = NULL, T_ValueSet *setTo = NULL) {
        TableTuple tempTuple = table->tempTuple();
        tempTuple.copy(tuple);
        int value = ::rand();
        tempTuple.setNValue(1, ValueFactory::getIntegerValue(value));
        if (setFrom != NULL) {
            setFrom->insert(*reinterpret_cast<const int64_t*>(tuple.address() + 1));
        }
        if (setTo != NULL) {
            setTo->insert(*reinterpret_cast<const int64_t*>(tempTuple.address() + 1));
        }
        table->updateTuple(tuple, tempTuple);
        m_tuplesUpdated++;
    }

    void doRandomInsert(PersistentTable *table, T_ValueSet *set = NULL) {
        addRandomUniqueTuples(table, 1, set);
        m_tuplesInserted++;
        m_tuplesInsertedInLastUndo++;
    }

    void doRandomUpdate(PersistentTable *table, T_ValueSet *setFrom = NULL, T_ValueSet *setTo = NULL) {
        TableTuple tuple(table->schema());
        if (tableutil::getRandomTuple(table, tuple)) {
            updateSpecificTuple(table, tuple, setFrom, setTo);
        }
    }

    void doRandomTableMutation(PersistentTable *table) {
        int rand = ::rand();
        int op = rand % 3;
        switch (op) {

            /*
             * Delete a tuple
             */
            case 0: {
                doRandomDelete(table);
                break;
            }

            /*
             * Insert a tuple
             */
            case 1: {
                doRandomInsert(table);
                break;
            }

            /*
             * Update a random tuple
             */
            case 2: {
                doRandomUpdate(table);
                break;
            }

            default:
                assert(false);
                break;
        }
    }

    bool doForcedCompaction(PersistentTable *table) {
        return table->doForcedCompaction();
    }

    void checkTuples(size_t tupleCount, const T_ValueSet& expected, const T_ValueSet& received) {
        std::vector<int64_t> diff;
        std::insert_iterator<std::vector<int64_t> > ii( diff, diff.begin());
        std::set_difference(expected.begin(), expected.end(), received.begin(), received.end(), ii);
        for (int jj = 0; jj < diff.size(); jj++) {
            int32_t *values = reinterpret_cast<int32_t*>(&diff[jj]);
            printf("Expected tuple was not received: %d/%d\n", values[0], values[1]);
        }

        diff.clear();
        ii = std::insert_iterator<std::vector<int64_t> >(diff, diff.begin());
        std::set_difference( received.begin(), received.end(), expected.begin(), expected.end(), ii);
        for (int jj = 0; jj < diff.size(); jj++) {
            int32_t *values = reinterpret_cast<int32_t*>(&diff[jj]);
            printf("Unexpected tuple received: %d/%d\n", values[0], values[1]);
        }

        size_t numTuples = 0;
        voltdb::TableIterator iterator = m_table->iterator();
        TableTuple tuple(m_table->schema());
        while (iterator.next(tuple)) {
            if (tuple.isDirty()) {
                printf("Tuple %d is active and dirty\n",
                       ValuePeeker::peekAsInteger(tuple.getNValue(0)));
            }
            numTuples++;
            if (tuple.isDirty()) {
                printf("Dirty tuple is %p, %d, %d\n", tuple.address(), ValuePeeker::peekAsInteger(tuple.getNValue(0)), ValuePeeker::peekAsInteger(tuple.getNValue(1)));
            }
            ASSERT_FALSE(tuple.isDirty());
        }
        if (tupleCount > 0 and numTuples != tupleCount) {
            printf("Expected %lu tuples, received %lu\n", numTuples, tupleCount);
            ASSERT_EQ(numTuples, tupleCount);
        }

        ASSERT_EQ(expected.size(), received.size());
        ASSERT_TRUE(expected == received);
    }

    void getTableValueSet(T_ValueSet &set) {
        voltdb::TableIterator iterator = m_table->iterator();
        TableTuple tuple(m_table->schema());
        while (iterator.next(tuple)) {
            const std::pair<T_ValueSet::iterator, bool> p =
                    set.insert(*reinterpret_cast<const int64_t*>(tuple.address() + 1));
            const bool inserted = p.second;
            if (!inserted) {
                int32_t primaryKey = ValuePeeker::peekAsInteger(tuple.getNValue(0));
                printf("Failed to insert %d\n", primaryKey);
            }
            if (!inserted) {
                ASSERT_TRUE(inserted);
            }
        }
    }

    // Avoid the need to make each individual test a friend by exposing
    // PersistentTable privates from here. Tests should call these methods
    // instead of adding them as friends.

    TBMap &getTableData() {
        return m_table->m_data;
    }

    PersistentTableSurgeon& getSurgeon() {
        return m_table->m_surgeon;
    }


    boost::unordered_set<TBPtr> &getBlocksPendingSnapshot() {
        return m_table->m_blocksPendingSnapshot;
    }

    boost::unordered_set<TBPtr> &getBlocksNotPendingSnapshot() {
        return m_table->m_blocksNotPendingSnapshot;
    }

    TBBucketPtrVector &getBlocksPendingSnapshotLoad() {
        return m_table->m_blocksPendingSnapshotLoad;
    }

    TBBucketPtrVector &getBlocksNotPendingSnapshotLoad() {
        return m_table->m_blocksNotPendingSnapshotLoad;
    }

    bool doActivateStream(TableStreamType streamType,
                          boost::shared_ptr<TableStreamerInterface> streamer,
                          std::vector<std::string> &predicateStrings,
                          bool skipInternalActivation) {
        m_outputStreams.reset(new TupleOutputStreamProcessor(m_serializationBuffer, sizeof(m_serializationBuffer)));
        m_outputStream = &m_outputStreams->at(0);
        return m_table->activateWithCustomStreamer(streamType,
                                                   HiddenColumnFilter::NONE,
                                                   streamer,
                                                   m_tableId,
                                                   predicateStrings,
                                                   skipInternalActivation);
    }

    int64_t doStreamMore(TableStreamType streamType) {
        return m_table->streamMore(*m_outputStreams, streamType, m_retPositions);
    }

    boost::shared_ptr<ElasticScanner> getElasticScanner() {
        return boost::shared_ptr<ElasticScanner>(new ElasticScanner(*m_table, m_table->m_surgeon.getData()));
    }

    void context(const std::string msg, ...) {
        va_list args;
        va_start(args, msg);
        vsnprintf(m_stage, sizeof m_stage, msg.c_str(), args);
        va_end(args);
    }

    void verror(const std::string msg, va_list args) {
        char buffer[256];
        vsnprintf(buffer, sizeof buffer, msg.c_str(), args);
        if (m_nerrors++ == 0) {
            std::cerr << std::endl;
        }
        std::cerr << "ERROR(";
        if (m_niteration >= 0) {
            std::cerr << "iteration=" << m_niteration << ": ";
        }
        std::cerr << m_stage << "): " << buffer << std::endl;
    }

    void error(const std::string msg, ...) {
        va_list args;
        va_start(args, msg);
        verror(msg, args);
        va_end(args);
    }

    void valueError(int32_t* pvalues, const std::string msg, ...) {
        if (m_showTuples) {
            std::cerr << std::endl << "=== Tuples ===" << std::endl;
            size_t n = 0;
            for (std::vector<int64_t>::iterator i = m_values.begin(); i != m_values.end(); ++i) {
                std::cerr << ++n << " " << *i << std::endl;
            }
            std::cerr << std::endl;
            m_showTuples = false;
        }
        int64_t value = *reinterpret_cast<const int64_t*>(pvalues);
        std::ostringstream os;
        os << msg << " value=" << value << "(" << pvalues[0] << "," << pvalues[1] << ") index=";
        std::map<int64_t,size_t>::const_iterator ifound = m_valueSet.find(value);
        if (ifound != m_valueSet.end()) {
            os << ifound->second;
        }
        else {
            os << "???";
        }
        os << " modulus=" << value % m_npartitions;
        va_list args;
        va_start(args, msg);
        verror(os.str(), args);
        va_end(args);
    }

    void diff(T_ValueSet set1, T_ValueSet set2) {
        std::vector<int64_t> diff;
        std::insert_iterator<std::vector<int64_t> > idiff( diff, diff.begin());
        std::set_difference(set1.begin(), set1.end(), set2.begin(), set2.end(), idiff);
        if (diff.size() <= MAX_DETAIL_COUNT) {
            for (int jj = 0; jj < diff.size(); jj++) {
                int32_t *values = reinterpret_cast<int32_t*>(&diff[jj]);
                valueError(values, "tuple");
            }
        }
        else {
            error("(%lu tuples)", diff.size());
        }
    }

    //=== Some convenience methods for building a JSON expression.

    // Assume non-ridiculous copy semantics for Object.
    // Structured JSON-building for readibility, not efficiency.

   static Json::Value expr_value_base(const std::string& type) {
        Json::Value value;
        value["TYPE"] = EXPRESSION_TYPE_VALUE_CONSTANT;
        value["VALUE_TYPE"] = static_cast<int>(stringToValue(type));
        value["VALUE_SIZE"] = 0;
        value["ISNULL"] = false;
        return value;
    }

    static Json::Value expr_value(const std::string& type, std::string& key, int data) {
        Json::Value value = expr_value_base(type);
        value[key.c_str()] = data;
        return value;
    }

    static Json::Value expr_value(const std::string& type, int ivalue) {
        std::string key = "VALUE";
        return expr_value(type, key, ivalue);
    }

    static Json::Value expr_value_tuple(const std::string& type,
                                        const std::string& tblname,
                                        int32_t colidx,
                                        const std::string& colname)
    {
        Json::Value value;
        value["TYPE"] = EXPRESSION_TYPE_VALUE_TUPLE;
        value["VALUE_TYPE"] = static_cast<int>(stringToValue(type));
        value["VALUE_SIZE"] = 0;
        value["TABLE_NAME"] = tblname;
        value["COLUMN_IDX"] = colidx;
        value["COLUMN_NAME"] = colname;
        value["COLUMN_ALIAS"] = Json::nullValue; // null
        return value;
    }

    static Json::Value expr_binary_op(const std::string& op,
                                      const std::string& type,
                                      const Json::Value& left,
                                      const Json::Value& right)
    {
        Json::Value value;
        value["TYPE"] = stringToExpression(op);
        value["VALUE_TYPE"] = static_cast<int>(stringToValue(type));
        value["VALUE_SIZE"] = 0;
        value["LEFT"] = left;
        value["RIGHT"] = right;
        return value;
    }

    void checkMultiCOW(T_ValueSet expected[], T_ValueSet actual[], bool doDelete, int ntotal, int nskipped) {
        // Summarize partitions with incorrect tuple counts.
        for (size_t ipart = 0; ipart < m_npartitions; ipart++) {
            context("check size: partition=%lu", ipart);
            if (expected[ipart].size() != actual[ipart].size()) {
                error("Size mismatch: expected=%lu actual=%lu",
                      expected[ipart].size(), actual[ipart].size());
            }
        }

        // Summarize partitions where expected and actual aren't equal.
        for (size_t ipart = 0; ipart < m_npartitions; ipart++) {
            context("check equality: partition=%lu", ipart);
            if (expected[ipart] != actual[ipart]) {
                error("Not equal");
            }
        }

        // Look for tuples that are missing from partitions.
        for (size_t ipart = 0; ipart < m_npartitions; ipart++) {
            context("missing: partition=%lu", ipart);
            diff(expected[ipart], actual[ipart]);
        }

        // Look for extra tuples that don't belong in partitions.
        for (size_t ipart = 0; ipart < m_npartitions; ipart++) {
            context("extra: partition=%lu", ipart);
            diff(actual[ipart], expected[ipart]);
        }

        // Check tuple diff for each predicate/partition.
        for (size_t ipart = 0; ipart < m_npartitions; ipart++) {
            context("check equality: partition=%lu", ipart);
            ASSERT_EQ(expected[ipart].size(), actual[ipart].size());
            ASSERT_TRUE(expected[ipart] == actual[ipart]);
        }

        // Check for dirty tuples.
        context("check dirty");
        int numTuples = 0;
        voltdb::TableIterator iterator = m_table->iterator();
        TableTuple tuple(m_table->schema());
        while (iterator.next(tuple)) {
            if (tuple.isDirty()) {
                error("Found tuple %d is active and dirty at end of COW",
                      ValuePeeker::peekAsInteger(tuple.getNValue(0)));
            }
            numTuples++;
            ASSERT_FALSE(tuple.isDirty());
        }

        // If deleting check the tuples remaining in the table.
        if (doDelete) {
            ASSERT_EQ(numTuples, nskipped);
        }
        else {
            ASSERT_EQ(numTuples, ntotal + (m_tuplesInserted - m_tuplesDeleted));
        }
        ASSERT_EQ(m_nerrors, 0);

    }

    static void dumpValueSet(const std::string &tag, const T_ValueSet &set) {
        std:: cout << "::: " << tag << " :::" << std::endl;
        if (set.size() >= 10) {
            std::cout << "  (" << set.size() << " items)" << std::endl;
        }
        else {
            for (T_ValueSet::const_iterator i = set.begin(); i != set.end(); ++i) {
                std::cout << *i << std::endl;
            }
        }
    }

    void checkScanner() {
        // Updates, inserts and deletes to tuples in blocks that were already
        // scanned are invisible, unless compaction moves their blocks around.
        // The checks have to be a little loose since we don't keep track of
        // which updates or deletes should be visible or not.

        // 1) Should be able to account for all scan returns in the initial,
        //    inserts or updatesTgt sets.
        T_ValueSet missing;
        for (T_ValueSet::const_iterator iter = m_returns.begin(); iter != m_returns.end(); ++iter) {
            T_Value value = *iter;
            if(m_initial.find(value) == m_initial.end() &&
               m_inserts.find(value) == m_inserts.end() &&
               m_updatesTgt.find(value) == m_updatesTgt.end()) {
                missing.insert(value);
            }
        }
        if (!missing.empty()) {
            std::cerr << std::endl << "ERROR: "
            << missing.size() << " scan tuple(s) received that can not be found"
            << " in the initial, insert or update (target) sets."
            << std::endl;
            dumpValueSet("unexpected returned tuple values", missing);
            dumpValueSet("initial tuple values", m_initial);
            dumpValueSet("inserted tuple values", m_inserts);
            dumpValueSet("updated tuple target values", m_updatesTgt);
            ASSERT_TRUE(missing.empty());
        }

        // 2) Should be able to account for all initial values in the returns,
        //    deletes or update (source) sets.
        for (T_ValueSet::const_iterator iter = m_initial.begin();
             iter != m_initial.end(); ++iter) {
            T_Value value = *iter;
            if(m_returns.find(value) == m_returns.end() &&
               m_deletes.find(value) == m_deletes.end() &&
               m_updatesSrc.find(value) == m_updatesSrc.end() &&
               m_shuffles.find(value) == m_shuffles.end()) {
                missing.insert(value);
            }
        }
        if (!missing.empty()) {
            /*
             * All initial tuples should have been returned by the scan, unless they
             * were deleted or updated (to have a different value).
             */
            std::cerr << std::endl << "ERROR: "
                      << missing.size() << " initial tuple(s) can not be found"
                      << " in the scan, delete, update (source), or compacted sets."
                      << std::endl;
            dumpValueSet("missing initial tuple values", missing);
            dumpValueSet("returned tuple values", m_returns);
            dumpValueSet("deleted tuple values", m_deletes);
            dumpValueSet("updated tuple source values", m_updatesSrc);
            ASSERT_TRUE(missing.empty());
        }

    }

    void checkIndex(const std::string &tag, ElasticIndex *index, StreamPredicateList &predicates, bool directKey) {
        ASSERT_NE(NULL, index);
        voltdb::TableIterator iterator = m_table->iterator();
        TableTuple tuple(m_table->schema());
        T_ValueSet accepted;
        T_ValueSet rejected;
        T_ValueSet missing;
        T_ValueSet extra;
        while (iterator.next(tuple)) {
            bool isAccepted = true;
            for (StreamPredicateList::iterator ipred = predicates.begin();
                 ipred != predicates.end(); ++ipred) {
                if (ipred->eval(&tuple).isFalse()) {
                    isAccepted = false;
                    break;
                }
            }
            T_Value value = *reinterpret_cast<T_Value*>(tuple.address() + 1);
            bool isIndexed;
            if (directKey) {
                // Direct key tests synthesize keys with NULL tuple addresses.
                int32_t hash = MurmurHash3_x64_128(tuple.address()+1, sizeof(int32_t), 0);
                ElasticIndexKey key(hash, (char *) NULL);
                isIndexed = index->exists(key);
            }
            else {
                isIndexed = index->has(*m_table, tuple);
            }
            if (isAccepted) {
                accepted.insert(value);
                if (!isIndexed) {
                    missing.insert(value);
                }
            }
            else {
                rejected.insert(value);
                if (isIndexed) {
                    extra.insert(value);
                }
            }
        }
        if (missing.size() > 0 || extra.size() > 0) {
            size_t ninitialMIA = 0;
            size_t ninsertedMIA = 0;
            size_t nupdatedMIA = 0;
            size_t nmovedMIA = 0;
            size_t wtf = 0;
            if (missing.size() > 0) {
                BOOST_FOREACH(T_Value value, missing) {
                    bool wasDeleted = m_deletes.find(value) != m_deletes.end();
                    bool wasUpdated = m_updatesSrc.find(value) != m_updatesSrc.end();
                    bool accountedFor = false;
                    if (!wasDeleted && !wasUpdated) {
                        if (m_initial.find(value) != m_initial.end()) {
                            ninitialMIA++;
                            accountedFor = true;
                        }
                        if (m_inserts.find(value) != m_inserts.end()) {
                            ninsertedMIA++;
                            accountedFor = true;
                        }
                        if (m_updatesTgt.find(value) != m_updatesTgt.end()) {
                            nupdatedMIA++;
                            accountedFor = true;
                        }
                        if (m_moved.find(value) != m_moved.end()) {
                            nmovedMIA++;
                        }
                    }
                    if (!accountedFor) {
                        wtf++;
                    }
                }
            }
            size_t ninitial = m_initial.size();
            size_t ninserted = m_inserts.size();
            size_t ndeleted = m_deletes.size();
            size_t nupdated = m_updatesTgt.size();
            size_t ntotal = ninitial + ninserted - ndeleted;
            size_t nactive = (size_t)m_table->activeTupleCount();
            size_t nrejected = rejected.size();
            size_t nexpected = nactive - nrejected;
            size_t nindexed = index->size();
            size_t nmissing = missing.size();
            size_t nextra = extra.size();
            size_t nmoved = m_moved.size();
            error("Bad index (%s) - tuple statistics:", tag.c_str());
            error("     Tuples: %lu = %lu+%lu-%lu (%lu updated)", ntotal, ninitial, ninserted, ndeleted, nupdated);
            error("   Expected: %lu = %lu-%lu", nexpected, nactive, nrejected);
            error("      Found: %lu", nindexed);
            error("      Moved: %lu", nmoved);
            error("    Missing: %lu (%lu/%lu/%lu/%lu/%lu)", nmissing, ninitialMIA, ninsertedMIA, nupdatedMIA, nmovedMIA, wtf);
            error("      Extra: %lu", nextra);
            /*BOOST_FOREACH(T_Value value, missing) {
             std::cout << value << std::endl;
             }*/
            ASSERT_EQ(0, nmissing);
            ASSERT_EQ(0, nextra);
        }
    }

    // Work around unsupported modulus operator with other integer operators:
    //    Should be: result = (value % nparts) == ipart
    //  Work-around: result = (value - ((value / nparts) * nparts)) == ipart
    std::string generatePredicateString(int32_t ipart, bool deleteForPredicate) {
        std::string tblname = m_table->name();
        int colidx = m_table->partitionColumn();
        std::string colname = m_table->columnName(colidx);
        Json::Value jsonTuple = expr_value_tuple("INTEGER", tblname, colidx, colname);
        Json::Value json =
        expr_binary_op("COMPARE_EQUAL", "INTEGER",
                       expr_binary_op("OPERATOR_MINUS", "INTEGER",
                                      jsonTuple,
                                      expr_binary_op("OPERATOR_MULTIPLY", "INTEGER",
                                                     expr_binary_op("OPERATOR_DIVIDE", "INTEGER",
                                                                    jsonTuple,
                                                                    expr_value("INTEGER", (int)m_npartitions)),
                                                     expr_value("INTEGER", (int)m_npartitions)
                                                     )
                                      ),
                       expr_value("INTEGER", (int)ipart)
                       );

        Json::Value predicateStuff;
        predicateStuff["triggersDelete"] = deleteForPredicate;
        predicateStuff["predicateExpression"] = json;

        Json::FastWriter writer;
        return writer.write(predicateStuff);
    }

    std::string generateHashRangePredicate(const T_HashRange& range) {
        T_HashRangeVector ranges;
        ranges.push_back(range);
        return generateHashRangePredicate(ranges);
    }

    std::string generateHashRangePredicate(const T_HashRangeVector& ranges) {
        int colidx = m_table->partitionColumn();
        Json::Value json;
        json["TYPE"] = EXPRESSION_TYPE_HASH_RANGE;
        json["VALUE_TYPE"] = static_cast<int>(ValueType::tBIGINT);
        json["VALUE_SIZE"] = 8;
        json["HASH_COLUMN"] = colidx;
        Json::Value array;
        for (size_t i = 0; i < ranges.size(); i++) {
            Json::Value range;
            range["RANGE_START"] = static_cast<Json::Int64>(ranges[i].first);
            range["RANGE_END"] = static_cast<Json::Int64>(ranges[i].second);
            array.append(range);
        }
        json["RANGES"] = array;
        Json::Value predicateStuff;
        predicateStuff["triggersDelete"] = false;
        predicateStuff["predicateExpression"] = json;

        Json::FastWriter writer;
        return writer.write(predicateStuff);
    }

    void resetTest() {
        m_tuplesInserted = m_tuplesDeleted = 0;
    }

    void parsePredicateList(const std::vector<std::string> &predicateStrings, StreamPredicateList &predicates) {
        std::ostringstream errmsg;
        std::vector<bool> deleteFlags;
        ASSERT_TRUE(predicates.parseStrings(predicateStrings, errmsg, deleteFlags));
    }

    boost::shared_ptr<ReferenceSerializeInputBE> getPredicateSerializeInput(const std::vector<std::string> &predicateStrings) {
        ReferenceSerializeOutput predicateOutput(m_predicateBuffer, 1024 * 256);
        predicateOutput.writeInt(1);
        for (std::vector<std::string>::const_iterator i = predicateStrings.begin();
             i != predicateStrings.end(); i++) {
            predicateOutput.writeTextString(*i);
        }
        return boost::shared_ptr<ReferenceSerializeInputBE>(
                new ReferenceSerializeInputBE(m_predicateBuffer, predicateOutput.position()));
    }

    voltdb::ElasticContext *getElasticContext() {
        voltdb::TableStreamer *streamer = dynamic_cast<voltdb::TableStreamer*>(m_table->m_tableStreamer.get());
        if (streamer != NULL) {
            BOOST_FOREACH(voltdb::TableStreamer::StreamPtr &streamPtr, streamer->m_streams) {
                if (streamPtr->m_streamType == TABLE_STREAM_ELASTIC_INDEX) {
                    voltdb::ElasticContext *context = dynamic_cast<ElasticContext*>(streamPtr->m_context.get());
                    if (context != NULL) {
                        return context;
                    }
                }
            }
        }
        return NULL;
    }

    voltdb::ElasticIndex *getElasticIndex() {
        return m_table->m_surgeon.m_index.get();
    }

    bool setElasticIndexTuplesPerCall(size_t nTuplesPerCall) {
        voltdb::ElasticContext *context = getElasticContext();
        if (context != NULL) {
            context->setTuplesPerCall(nTuplesPerCall);
            return true;
        }
        return false;
    }

    void streamElasticIndex(std::vector<std::string> &predicateStrings, bool checkCalls) {
        boost::shared_ptr<ReferenceSerializeInputBE> predicateInput = getPredicateSerializeInput(predicateStrings);
        bool ok = m_table->activateStream(TABLE_STREAM_ELASTIC_INDEX, HiddenColumnFilter::NONE, 0, m_tableId, *predicateInput);
        ASSERT_TRUE(ok);

        // Force index streaming to need multiple streamMore() calls.
        voltdb::ElasticContext *context = getElasticContext();
        ASSERT_NE(NULL, context);
        bool success = setElasticIndexTuplesPerCall(20);
        ASSERT_TRUE(success);
        std::vector<int> retPositionsElastic;
        TupleOutputStreamProcessor outputStreamsElastic(m_serializationBuffer, sizeof(m_serializationBuffer));
        size_t nCalls = 0;
        while (m_table->streamMore(outputStreamsElastic, TABLE_STREAM_ELASTIC_INDEX, retPositionsElastic) != 0) {
            nCalls++;
        }
        // Make sure we forced more than one streamMore() call.
        if (checkCalls) {
            ASSERT_LE(2, nCalls);
        }
    }

    void streamSnapshot(int numMutationsDuring, int numMutationsAfter, T_ValueSet &COWTuples, int &totalInserted) {
        char config[4];
        ::memset(config, 0, 4);
        ::memset(config, 0, 4);
        ReferenceSerializeInputBE predicateInput(config, 4);

        totalInserted = 0;

        m_table->activateStream(TABLE_STREAM_SNAPSHOT, HiddenColumnFilter::NONE, 0, m_tableId, predicateInput);

        while (true) {
            TupleOutputStreamProcessor outputStreams(m_serializationBuffer, sizeof(m_serializationBuffer));
            TupleOutputStream &outputStream = outputStreams.at(0);
            std::vector<int> retPositions;
            int64_t remaining = m_table->streamMore(outputStreams, TABLE_STREAM_SNAPSHOT, retPositions);
            if (remaining >= 0) {
                ASSERT_EQ(outputStreams.size(), retPositions.size());
            }
            const int serialized = static_cast<int>(outputStream.position());
            if (serialized == 0) {
                break;
            }
            for (size_t ii = sizeof(int32_t)*3; // skip partition id, row count, and first tuple length
                 ii + sizeof(int64_t) <= serialized;
                 ii += m_tupleWidth + sizeof(int32_t)) {
                int values[2];
                values[0] = ntohl(*reinterpret_cast<const int32_t*>(&m_serializationBuffer[ii]));
                values[1] = ntohl(*reinterpret_cast<const int32_t*>(&m_serializationBuffer[ii + 4]));
                void *valuesVoid = reinterpret_cast<void*>(values);
                const int64_t *values64 = reinterpret_cast<const int64_t*>(valuesVoid);
                const bool inserted = COWTuples.insert(*values64).second;
                if (!inserted) {
                    error("Failed: total inserted %d, with values %d and %d\n", totalInserted, values[0], values[1]);
                }
                ASSERT_TRUE(inserted);
                totalInserted++;
            }
            for (int jj = 0; jj < numMutationsDuring; jj++) {
                doRandomTableMutation(m_table);
            }
        }

        // Do some extra mutations for good luck.
        for (int jj = 0; jj < numMutationsAfter; jj++) {
            doRandomTableMutation(m_table);
        }
    }

    boost::shared_ptr<ReferenceSerializeInputBE> getHashRangePredicateInput(const T_HashRange &testRange) {
        // Set up the hash range predicate.
        ReferenceSerializeOutput hashRangeOutput(m_hashRangeBuffer, 1024 * 256);
        std::ostringstream hashRangeString;
        hashRangeString << testRange.first << ':' << testRange.second;
        hashRangeOutput.writeInt(1);
        hashRangeOutput.writeTextString(hashRangeString.str());

        return boost::shared_ptr<ReferenceSerializeInputBE>(
                new ReferenceSerializeInputBE(m_hashRangeBuffer, hashRangeOutput.position()));
    }

    void materializeIndex(ElasticIndex &index, const T_HashRange &testRange, bool undo, size_t &totalInserted) {
        boost::shared_ptr<ReferenceSerializeInputBE> predicateInput = getHashRangePredicateInput(testRange);

        m_engine->setUndoToken(m_undoToken);
        bool activated = m_table->activateStream(TABLE_STREAM_ELASTIC_INDEX_READ, HiddenColumnFilter::NONE,
                                                 0, m_tableId, *predicateInput);
        ASSERT_TRUE(activated);

        totalInserted = 0;
        while (true) {
            memset(m_serializationBuffer, 0, sizeof(m_serializationBuffer));
            TupleOutputStreamProcessor outputStreams(m_serializationBuffer, sizeof(m_serializationBuffer));
            TupleOutputStream &outputStream = outputStreams.at(0);
            std::vector<int> retPositions;
            int64_t remaining = m_table->streamMore(outputStreams, TABLE_STREAM_ELASTIC_INDEX_READ, retPositions);
            if (remaining >= 0) {
                ASSERT_EQ(outputStreams.size(), retPositions.size());
            }
            const int serialized = static_cast<int>(outputStream.position());
            if (serialized == 0) {
                break;
            }

            // Trying to clear an index that's not drained will fail, but should be side-effect free
            clearIndex(testRange, false);

            for (size_t ii = sizeof(int32_t)*3; // skip partition id, row count, and first tuple length
                 ii + sizeof(int32_t) < serialized; // WHY strictly < ? --paul
                 ii += m_tupleWidth + sizeof(int32_t)) {
                int32_t value = ntohl(*reinterpret_cast<const int32_t*>(&m_serializationBuffer[ii]));
                int32_t hash = MurmurHash3_x64_128(&value, sizeof(value), 0);
                ElasticIndexKey key(hash, (char *) NULL);
                bool inserted = index.add(key);
                ASSERT_TRUE(inserted);
                totalInserted++;
            }
        }

        if (undo) {
            m_engine->undoUndoToken(m_undoToken);
        }
        else {
            m_engine->releaseUndoToken(m_undoToken, false);
        }
        ExecutorContext::getExecutorContext()->setupForPlanFragments(m_engine->getCurrentUndoQuantum(),
                                                                     0, 0, 0, 0, false);
        m_undoToken++;
    }

    void clearIndex(const T_HashRange &testRange, bool expected) {
        boost::shared_ptr<ReferenceSerializeInputBE> predicateInput = getHashRangePredicateInput(testRange);
        bool activated = m_table->activateStream(TABLE_STREAM_ELASTIC_INDEX_CLEAR, HiddenColumnFilter::NONE,
                                                 0, m_tableId, *predicateInput);
        ASSERT_EQ(expected,activated);
    }

    void deleteBlockPendingSnapshot(int nBlocks, int dirtyBlock) {
        // Re-create conditions of ENG-19517 where block pending snapshot is removed
        initTable(1, 0);

        ASSERT_TRUE(nBlocks >= 3 && dirtyBlock >= 0 && dirtyBlock < nBlocks);
        int tuplesPerBlock = m_table->getTuplesPerBlock();
        int tupleCount = nBlocks * tuplesPerBlock;
        addRandomUniqueTuples( m_table, tupleCount);
        // cout << "Added " << tupleCount << " to table with " << tuplesPerBlock << " tuples/block" << endl;

        voltdb::TableIterator iterator = m_table->iterator();

        TBMap blocks(getTableData());
        // cout << "Table has " << blocks.size() << " blocks" << endl;

        // Delete all rows from dirty block, EXCEPT THE LAST ONE, and memorize this last one
        // as the tuple we're going to dirty and delete after COW streamer started.
        TableTuple tuple(m_table->schema());
        int deliter = 0;
        int minDel = dirtyBlock * tuplesPerBlock;
        int maxDel = minDel + tuplesPerBlock;
        while (deliter < maxDel) {
            ASSERT_TRUE(iterator.next(tuple));
            if (deliter == maxDel - 1) {
                break;
            }
            else if (deliter >= minDel) {
                m_table->deleteTuple(tuple, true);
            }
            deliter++;
        }
        // cout << "min " << minDel << ", max " << maxDel << ", dirty " << deliter << endl;
        TableTuple dirtyTuple = tuple;
        // cout << "Table has " << getTableData().size() << " blocks after deletion" << endl;
        ASSERT_EQ(getTableData().size(), nBlocks);

        // Memorize table value set before starting snapshot
        T_ValueSet originalTuples;
        getTableValueSet(originalTuples);

        // Activate a snapshot with dirty block with 1 tuple and other blocks full
        char config[4];
        ::memset(config, 0, 4);
        ReferenceSerializeInputBE input(config, 4);
        m_table->activateStream(TABLE_STREAM_SNAPSHOT, HiddenColumnFilter::NONE, 0, m_tableId, input);

        // Modify and delete the dirty tuple, verify the block was freed
        updateSpecificTuple(m_table, dirtyTuple);
        m_table->deleteTuple(dirtyTuple, true);
        // cout << "Table has " << getTableData().size() << " blocks after dirty tuple deletion" << endl;
        ASSERT_EQ(getTableData().size(), nBlocks - 1);

        // Verify COW iteration is correct
        T_ValueSet COWTuples;
        char serializationBuffer[BUFFER_SIZE];
        while (true) {
            TupleOutputStreamProcessor outputStreams(serializationBuffer, sizeof(serializationBuffer));
            TupleOutputStream &outputStream = outputStreams.at(0);
            std::vector<int> retPositions;
            int64_t remaining = m_table->streamMore(outputStreams, TABLE_STREAM_SNAPSHOT, retPositions);
            if (remaining >= 0) {
                ASSERT_EQ(outputStreams.size(), retPositions.size());
            }
            const size_t serialized = outputStream.position();
            if (serialized == 0) {
                break;
            }
            for (size_t ii = sizeof(int32_t)*3; // skip partition id, row count, and first tuple length
                 ii + sizeof(int64_t) <= serialized;
                 ii += m_tupleWidth + sizeof(int32_t)) {
                int32_t values[2];
                values[0] = ntohl(*reinterpret_cast<const int32_t*>(&serializationBuffer[ii]));
                values[1] = ntohl(*reinterpret_cast<const int32_t*>(&serializationBuffer[ii + 4]));
                // the following rediculous cast is to placate our gcc treat warnings as errors affliction
                void *valuesVoid = reinterpret_cast<void*>(values);
                const int64_t *values64 = reinterpret_cast<const int64_t*>(valuesVoid);
                const bool inserted = COWTuples.insert(*values64).second;
                if (!inserted) {
                    printf("Failed, with values %d and %d\n", values[0], values[1]);
                }
                ASSERT_TRUE(inserted);
            }
        }
        checkTuples((nBlocks - 1) * tuplesPerBlock, originalTuples, COWTuples);
    }

    voltdb::VoltDBEngine *m_engine;
    voltdb::TupleSchema *m_tableSchema;
    voltdb::PersistentTable *m_table;
    voltdb::MockDRTupleStream drStream;
    std::vector<std::string> m_columnNames;
    std::vector<voltdb::ValueType> m_tableSchemaTypes;
    std::vector<int32_t> m_tableSchemaColumnSizes;
    std::vector<bool> m_tableSchemaAllowNull;
    std::vector<int> m_primaryKeyIndexColumns;
    char signature[20];
    char m_serializationBuffer[BUFFER_SIZE];
    char m_predicateBuffer[1024 * 256];
    char m_hashRangeBuffer[1024 * 256];
    boost::shared_ptr<TupleOutputStreamProcessor> m_outputStreams;
    TupleOutputStream *m_outputStream;
    std::vector<int> m_retPositions;

    int32_t m_tuplesInserted;
    int32_t m_tuplesUpdated;
    int32_t m_tuplesDeleted;

    int32_t m_tuplesInsertedInLastUndo;
    int32_t m_tuplesDeletedInLastUndo;

    int64_t m_undoToken;

    size_t m_tupleWidth;

    CatalogId m_tableId;

    size_t m_npartitions;
    int m_niteration;
    char m_stage[256];
    size_t m_nerrors;
    std::vector<int64_t> m_values;
    std::map<int64_t,size_t> m_valueSet;
    bool m_showTuples;

    // Value sets used for checking results.
    T_ValueSet m_initial;
    T_ValueSet m_inserts;
    T_ValueSet m_updatesSrc;
    T_ValueSet m_updatesTgt;
    T_ValueSet m_deletes;
    T_ValueSet m_moved;
    T_ValueSet m_returns;
    T_ValueSet m_shuffles;
};

TEST_F(CopyOnWriteTest, CopyOnWriteIterator) {
    initTable(1, 0);

    int tupleCount = TUPLE_COUNT;
    addRandomUniqueTuples( m_table, tupleCount);

    voltdb::TableIterator iterator = m_table->iterator();
    TBMap blocks(getTableData());
    getBlocksPendingSnapshot().swap(getBlocksNotPendingSnapshot());
    getBlocksPendingSnapshotLoad().swap(getBlocksNotPendingSnapshotLoad());
    voltdb::CopyOnWriteIterator COWIterator(m_table, &getSurgeon());
    TableTuple tuple(m_table->schema());
    TableTuple COWTuple(m_table->schema());

    int iteration = 0;
    while (true) {
        iteration++;
        if (!iterator.next(tuple)) {
            break;
        }
        ASSERT_TRUE(COWIterator.next(COWTuple));

        if (tuple.address() != COWTuple.address()) {
            printf("Failed in iteration %d with %p and %p\n", iteration, tuple.address(), COWTuple.address());
        }
        ASSERT_EQ(tuple.address(), COWTuple.address());
    }
    ASSERT_FALSE(COWIterator.next(COWTuple));
}

TEST_F(CopyOnWriteTest, TestTableTupleFlags) {
    initTable(1, 0);
    char storage[9];
    std::memset(storage, 0, 9);
    TableTuple tuple(m_table->schema());
    tuple.move(storage);

    tuple.setActiveFalse();
    tuple.setDirtyTrue();
    ASSERT_FALSE(tuple.isActive());
    ASSERT_TRUE(tuple.isDirty());

    tuple.setActiveTrue();
    ASSERT_TRUE(tuple.isDirty());
    ASSERT_TRUE(tuple.isActive());

    tuple.setDirtyFalse();
    ASSERT_TRUE(tuple.isActive());
    ASSERT_FALSE(tuple.isDirty());
}

// Simple test that performs snapshot activation on empty table, inserts tuples and calls stream more tuples
TEST_F(CopyOnWriteTest, TestTupleInsertionBetweenSnapshotActivateFinish) {
    initTable(1, 0);
    int tupleCount = 4;

    // Empty table has an assigned allocated tuple storage
    ASSERT_EQ(1, m_table->allocatedBlockCount());
    char config[4];
    ::memset(config, 0, 4);
    ReferenceSerializeInputBE input(config, 4);
    // activate snapshot
    m_table->activateStream(TABLE_STREAM_SNAPSHOT, HiddenColumnFilter::NONE, 0, m_tableId, input);
    // insert tuples
    addRandomUniqueTuples(m_table, tupleCount);
    // do work - start taking snapshot of the table
    char serializationBuffer[BUFFER_SIZE];
    TupleOutputStreamProcessor outputStreams(serializationBuffer, sizeof(serializationBuffer));
    std::vector<int> retPositions;
    int64_t remaining = m_table->streamMore(outputStreams, TABLE_STREAM_SNAPSHOT, retPositions);
    // no work done by snapshot as the table was empty
    ASSERT_EQ(0, remaining);
    ASSERT_EQ(outputStreams.size(), retPositions.size());
    // check the # tuple insertion count is reflected correctly
    ASSERT_EQ(tupleCount, m_table->visibleTupleCount());
}

TEST_F(CopyOnWriteTest, BigTest) {
    initTable(1, 0);
    int tupleCount = TUPLE_COUNT;
    addRandomUniqueTuples( m_table, tupleCount);
    for (int qq = 0; qq < NUM_REPETITIONS; qq++) {
        T_ValueSet originalTuples;
        getTableValueSet(originalTuples);

        char config[4];
        ::memset(config, 0, 4);
        ReferenceSerializeInputBE input(config, 4);

        m_table->activateStream(TABLE_STREAM_SNAPSHOT, HiddenColumnFilter::NONE, 0, m_tableId, input);

        T_ValueSet COWTuples;
        char serializationBuffer[BUFFER_SIZE];
        int totalInserted = 0;
        while (true) {
            TupleOutputStreamProcessor outputStreams(serializationBuffer, sizeof(serializationBuffer));
            TupleOutputStream &outputStream = outputStreams.at(0);
            std::vector<int> retPositions;
            int64_t remaining = m_table->streamMore(outputStreams, TABLE_STREAM_SNAPSHOT, retPositions);
            if (remaining >= 0) {
                ASSERT_EQ(outputStreams.size(), retPositions.size());
            }
            const size_t serialized = outputStream.position();
            if (serialized == 0) {
                break;
            }
            for (size_t ii = sizeof(int32_t)*3; // skip partition id, row count, and first tuple length
                 ii + sizeof(int64_t) <= serialized;
                 ii += m_tupleWidth + sizeof(int32_t)) {
                int32_t values[2];
                values[0] = ntohl(*reinterpret_cast<const int32_t*>(&serializationBuffer[ii]));
                values[1] = ntohl(*reinterpret_cast<const int32_t*>(&serializationBuffer[ii + 4]));
                // the following rediculous cast is to placate our gcc treat warnings as errors affliction
                void *valuesVoid = reinterpret_cast<void*>(values);
                const int64_t *values64 = reinterpret_cast<const int64_t*>(valuesVoid);
                const bool inserted = COWTuples.insert(*values64).second;
                if (!inserted) {
                    printf("Failed in iteration %d, total inserted %d, with values %d and %d\n", qq, totalInserted, values[0], values[1]);
                }
                ASSERT_TRUE(inserted);
                totalInserted++;
            }
            for (int jj = 0; jj < NUM_MUTATIONS; jj++) {
                doRandomTableMutation(m_table);
            }
        }

        checkTuples(tupleCount + (m_tuplesInserted - m_tuplesDeleted), originalTuples, COWTuples);
    }
}

TEST_F(CopyOnWriteTest, BigTestWithUndo) {
    initTable(1, 0);
    int tupleCount = TUPLE_COUNT;
    addRandomUniqueTuples( m_table, tupleCount);
    m_engine->setUndoToken(0);
    ExecutorContext::getExecutorContext()->setupForPlanFragments(m_engine->getCurrentUndoQuantum(),
                                                                 0, 0, 0, 0, false);
    for (int qq = 0; qq < NUM_REPETITIONS; qq++) {
        T_ValueSet originalTuples;
        voltdb::TableIterator iterator = m_table->iterator();
        TableTuple tuple(m_table->schema());
        while (iterator.next(tuple)) {
            const std::pair<T_ValueSet::iterator, bool> p =
            originalTuples.insert(*reinterpret_cast<const int64_t*>(tuple.address() + 1));
            const bool inserted = p.second;
            if (!inserted) {
                int32_t primaryKey = ValuePeeker::peekAsInteger(tuple.getNValue(0));
                printf("Failed to insert %d\n", primaryKey);
            }
            ASSERT_TRUE(inserted);
        }

        char config[4];
        ::memset(config, 0, 4);
        ReferenceSerializeInputBE input(config, 4);
        m_table->activateStream(TABLE_STREAM_SNAPSHOT, HiddenColumnFilter::NONE, 0, m_tableId, input);

        T_ValueSet COWTuples;
        char serializationBuffer[BUFFER_SIZE];
        int totalInserted = 0;
        while (true) {
            TupleOutputStreamProcessor outputStreams(serializationBuffer, sizeof(serializationBuffer));
            TupleOutputStream &outputStream = outputStreams.at(0);
            std::vector<int> retPositions;
            int64_t remaining = m_table->streamMore(outputStreams, TABLE_STREAM_SNAPSHOT, retPositions);
            if (remaining >= 0) {
                ASSERT_EQ(outputStreams.size(), retPositions.size());
            }
            const int serialized = static_cast<int>(outputStream.position());
            if (serialized == 0) {
                break;
            }
            for (size_t ii = sizeof(int32_t)*3; // skip partition id, row count, and first tuple length
                 ii + sizeof(int64_t) <= serialized;
                 ii += m_tupleWidth + sizeof(int32_t)) {
                int values[2];
                values[0] = ntohl(*reinterpret_cast<const int32_t*>(&serializationBuffer[ii]));
                values[1] = ntohl(*reinterpret_cast<const int32_t*>(&serializationBuffer[ii + 4]));
                // the following rediculous cast is to placate our gcc treat warnings as errors affliction
                void *valuesVoid = reinterpret_cast<void*>(values);
                const int64_t *values64 = reinterpret_cast<const int64_t*>(valuesVoid);
                const bool inserted = COWTuples.insert(*values64).second;
                if (!inserted) {
                    printf("Failed in iteration %d with values %d and %d\n", totalInserted, values[0], values[1]);
                }
                ASSERT_TRUE(inserted);
                totalInserted++;
            }
            for (int jj = 0; jj < NUM_MUTATIONS; jj++) {
                doRandomTableMutation(m_table);
            }
            doRandomUndo();
        }

        checkTuples(tupleCount + (m_tuplesInserted - m_tuplesDeleted), originalTuples, COWTuples);
    }
}

TEST_F(CopyOnWriteTest, BigTestUndoEverything) {
    initTable(1, 0);
    int tupleCount = TUPLE_COUNT;
    addRandomUniqueTuples( m_table, tupleCount);
    m_engine->setUndoToken(0);
    ExecutorContext::getExecutorContext()->setupForPlanFragments(m_engine->getCurrentUndoQuantum(),
                                                                 0, 0, 0, 0, false);
    for (int qq = 0; qq < NUM_REPETITIONS; qq++) {
        T_ValueSet originalTuples;
        voltdb::TableIterator iterator = m_table->iterator();
        TableTuple tuple(m_table->schema());
        while (iterator.next(tuple)) {
            const std::pair<T_ValueSet::iterator, bool> p =
            originalTuples.insert(*reinterpret_cast<const int64_t*>(tuple.address() + 1));
            const bool inserted = p.second;
            if (!inserted) {
                int32_t primaryKey = ValuePeeker::peekAsInteger(tuple.getNValue(0));
                printf("Failed to insert %d\n", primaryKey);
            }
            ASSERT_TRUE(inserted);
        }

        char config[4];
        ::memset(config, 0, 4);
        ReferenceSerializeInputBE input(config, 4);
        m_table->activateStream(TABLE_STREAM_SNAPSHOT, HiddenColumnFilter::NONE, 0, m_tableId, input);

        T_ValueSet COWTuples;
        char serializationBuffer[BUFFER_SIZE];
        int totalInserted = 0;
        while (true) {
            TupleOutputStreamProcessor outputStreams(serializationBuffer, sizeof(serializationBuffer));
            TupleOutputStream &outputStream = outputStreams.at(0);
            std::vector<int> retPositions;
            int64_t remaining = m_table->streamMore(outputStreams, TABLE_STREAM_SNAPSHOT, retPositions);
            if (remaining >= 0) {
                ASSERT_EQ(outputStreams.size(), retPositions.size());
            }
            const int serialized = static_cast<int>(outputStream.position());
            if (serialized == 0) {
                break;
            }
            for (size_t ii = sizeof(int32_t)*3; // skip partition id, row count, and first tuple length
                 ii + sizeof(int64_t) <= serialized;
                 ii += m_tupleWidth + sizeof(int32_t)) {
                int values[2];
                values[0] = ntohl(*reinterpret_cast<const int32_t*>(&serializationBuffer[ii]));
                values[1] = ntohl(*reinterpret_cast<const int32_t*>(&serializationBuffer[ii + 4]));
                // the following rediculous cast is to placate our gcc treat warnings as errors affliction
                void *valuesVoid = reinterpret_cast<void*>(values);
                const int64_t *values64 = reinterpret_cast<const int64_t*>(valuesVoid);
                const bool inserted = COWTuples.insert(*values64).second;
                if (!inserted) {
                    printf("Failed in iteration %d with values %d and %d\n", totalInserted, values[0], values[1]);
                }
                ASSERT_TRUE(inserted);
                totalInserted++;
            }
            for (int jj = 0; jj < NUM_MUTATIONS; jj++) {
                doRandomTableMutation(m_table);
            }
            m_engine->undoUndoToken(m_undoToken);
            m_engine->setUndoToken(++m_undoToken);
            ExecutorContext::getExecutorContext()->setupForPlanFragments(m_engine->getCurrentUndoQuantum(),
                                                                         0, 0, 0, 0, false);
        }

        checkTuples(0, originalTuples, COWTuples);
    }
}

/**
 * Exercise the multi-Stream.
 */
TEST_F(CopyOnWriteTest, MultiStream) {

    // Constants
    const int32_t npartitions = 7;
    const int tupleCount = TUPLE_COUNT;

    initTable(npartitions, 0);
    addRandomUniqueTuples(m_table, tupleCount);

    for (size_t iteration = 0; iteration < NUM_REPETITIONS; iteration++) {

        // The last repetition does the delete after streaming.
        bool doDelete = (iteration == NUM_REPETITIONS - 1);

        iterate();

        int totalInserted = 0;              // Total tuple counter.
        boost::scoped_array<char> buffers[npartitions];   // Stream buffers.
        std::vector<std::string> strings(npartitions);  // Range strings.
        T_ValueSet expected[npartitions]; // Expected tuple values by partition.
        T_ValueSet actual[npartitions];   // Actual tuple values by partition.
        int totalSkipped = 0;

        // Prepare streams by generating ranges and range strings based on
        // the desired number of partitions/predicates.
        // Since integer hashes use a simple modulus we just need to provide
        // the partition number for the range.
        // Also prepare a buffer for each stream.
        // Skip one partition to make it interesting.
        int32_t skippedPartition = npartitions / 2;
        for (int32_t i = 0; i < npartitions; i++) {
            buffers[i].reset(new char[BUFFER_SIZE]);
            if (i != skippedPartition) {
                strings[i] = generatePredicateString(i, doDelete);
            }
            else {
                strings[i] = generatePredicateString(-1, doDelete);
            }
        }

        char buffer[1024 * 256];
        ReferenceSerializeOutput output(buffer, 1024 * 256);
        output.writeInt(npartitions);
        for (std::vector<std::string>::iterator i = strings.begin(); i != strings.end(); i++) {
            output.writeTextString(*i);
        }

        context("precalculate");

        // Map original tuples to expected partitions.
        voltdb::TableIterator iterator = m_table->iterator();
        int partCol = m_table->partitionColumn();
        TableTuple tuple(m_table->schema());
        while (iterator.next(tuple)) {
            int64_t value = *reinterpret_cast<const int64_t*>(tuple.address() + 1);
            int32_t ipart = (int32_t)(ValuePeeker::peekAsRawInt64(tuple.getNValue(partCol)) % npartitions);
            if (ipart != skippedPartition) {
                bool inserted = expected[ipart].insert(value).second;
                if (!inserted) {
                    int32_t primaryKey = ValuePeeker::peekAsInteger(tuple.getNValue(0));
                    error("Duplicate primary key %d iteration=%lu", primaryKey, iteration);
                }
                ASSERT_TRUE(inserted);
            }
            else {
                totalSkipped++;
            }
        }

        context("activate");

        ReferenceSerializeInputBE input(buffer, output.position());
        bool success = m_table->activateStream(TABLE_STREAM_SNAPSHOT, HiddenColumnFilter::NONE, 0, m_tableId, input);
        if (!success) {
            error("COW was previously activated");
        }
        ASSERT_TRUE(success);

        int64_t remaining = tupleCount;
        while (remaining > 0) {

            // Prepare output streams and their buffers.
            TupleOutputStreamProcessor outputStreams;
            for (int32_t i = 0; i < npartitions; i++) {
                outputStreams.add((void*)buffers[i].get(), BUFFER_SIZE);
            }

            std::vector<int> retPositions;
            remaining = m_table->streamMore(outputStreams, TABLE_STREAM_SNAPSHOT, retPositions);
            if (remaining >= 0) {
                ASSERT_EQ(outputStreams.size(), retPositions.size());
            }

            // Per-predicate iterators.
            TupleOutputStreamProcessor::iterator outputStream = outputStreams.begin();

            // Record the final result of streaming to each partition/predicate.
            for (size_t ipart = 0; ipart < npartitions; ipart++) {

                context("serialize: partition=%lu remaining=%lld", ipart, remaining);

                const int serialized = static_cast<int>(outputStream->position());
                const char* serializationBuffer = buffers[ipart].get();
                for (size_t ii = sizeof(int32_t)*3; // skip partition id, row count, and first tuple length
                     ii + sizeof(int64_t) <= serialized;
                     ii += m_tupleWidth + sizeof(int32_t)) {
                    int32_t values[2];
                    values[0] = ntohl(*reinterpret_cast<const int32_t*>(&serializationBuffer[ii]));
                    values[1] = ntohl(*reinterpret_cast<const int32_t*>(&serializationBuffer[ii + 4]));
                    // the following rediculous cast is to placate our gcc treat warnings as errors affliction
                    void *valuesVoid = reinterpret_cast<void*>(values);
                    const int64_t *values64 = reinterpret_cast<const int64_t*>(valuesVoid);
                    const bool inserted = actual[ipart].insert(*values64).second;
                    if (!inserted) {
                        valueError(values, "Buffer duplicate: ipart=%lu totalInserted=%d ii=%d",
                                   ipart, totalInserted, ii);
                    }
                    ASSERT_TRUE(inserted);
                    totalInserted++;
                }

                // Mozy along to the next predicate/partition.
                // Do a silly cross-check that the iterator doesn't end prematurely.
                ++outputStream;
                ASSERT_TRUE(ipart == npartitions - 1 || outputStream != outputStreams.end());
            }

            // Mutate the table.
            if (!doDelete) {
                for (size_t imutation = 0; imutation < NUM_MUTATIONS; imutation++) {
                    doRandomTableMutation(m_table);
                }
            }
        }

        checkMultiCOW(expected, actual, doDelete, tupleCount, totalSkipped);
    }
}

/*
 * Test for the ENG-4524 edge condition where serializeMore() yields on
 * precisely the last tuple which had caused the loop to skip the last call to
 * the iterator next() method. Need to rig this test with the appropriate
 * buffer size and tuple count to force the edge condition.
 *
 * The buffer has to be a smidge larger than what is needed to hold the tuples
 * so that TupleOutputStreamProcessor::writeRow() discovers it can't fit
 * another tuple immediately after writing the last one. It doesn't know how
 * many there are so it yields even if no more tuples will be delivered.
 */
TEST_F(CopyOnWriteTest, BufferBoundaryCondition) {
    const size_t tupleCount = 3;
    const size_t bufferSize = (sizeof(int32_t) * 3) + ((m_tupleWidth + sizeof(int32_t)) * tupleCount);
    initTable(1, 0);
    TableTuple tuple(m_table->schema());
    addRandomUniqueTuples(m_table, tupleCount);
    size_t origPendingCount = m_table->getBlocksNotPendingSnapshotCount();
    // This should succeed in one call to serializeMore().
    char serializationBuffer[bufferSize];
    char config[4];
    ::memset(config, 0, 4);
    ReferenceSerializeInputBE input(config, 4);
    m_table->activateStream(TABLE_STREAM_SNAPSHOT, HiddenColumnFilter::NONE, 0, m_tableId, input);
    TupleOutputStreamProcessor outputStreams(serializationBuffer, bufferSize);
    std::vector<int> retPositions;
    int64_t remaining = m_table->streamMore(outputStreams, TABLE_STREAM_SNAPSHOT, retPositions);
    if (remaining >= 0) {
        ASSERT_EQ(outputStreams.size(), retPositions.size());
    }
    ASSERT_EQ(0, remaining);
    // Expect the same pending count, because it should get reset when
    // serialization finishes cleanly.
    size_t curPendingCount = m_table->getBlocksNotPendingSnapshotCount();
    ASSERT_EQ(origPendingCount, curPendingCount);
}

// Test reproducing ENG-19517, delete middle block pending snapshot
TEST_F(CopyOnWriteTest, DeleteBlock_ENG_19517_middle) {
    deleteBlockPendingSnapshot(5, 2);
}

// Test reproducing ENG-19517, delete first block pending snapshot
TEST_F(CopyOnWriteTest, DeleteBlock_ENG_19517_first) {
    deleteBlockPendingSnapshot(5, 0);
}

// Test reproducing ENG-19517, delete last block pending snapshot
TEST_F(CopyOnWriteTest, DeleteBlock_ENG_19517_last) {
    deleteBlockPendingSnapshot(5, 4);
}

/**
 * Dummy TableStreamer for intercepting and tracking tuple notifications.
 */
class DummyTableStreamer : public TableStreamerInterface {
public:
    DummyTableStreamer(CopyOnWriteTest &test, int32_t partitionId, TableStreamType type) :
        m_test(test), m_partitionId(partitionId), m_type(type) {}

    virtual bool activateStream(PersistentTableSurgeon &surgeon,
                                TableStreamType streamType,
                                const HiddenColumnFilter &filter,
                                const std::vector<std::string> &predicateStrings) {
        return false;
    }

    virtual int64_t streamMore(TupleOutputStreamProcessor &outputStreams,
                               TableStreamType streamType,
                               std::vector<int> &retPositions) { return 0; }

    virtual int32_t getPartitionID() const { return m_partitionId; }

    virtual bool canSafelyFreeTuple(TableTuple &tuple) const { return true; }

    virtual TableStreamerContextPtr findStreamContext(TableStreamType streamType) { return TableStreamerContextPtr(); }

    virtual bool notifyTupleInsert(TableTuple &tuple) { return false; }

    virtual bool notifyTupleUpdate(TableTuple &tuple) { return false; }

    virtual bool notifyTupleDelete(TableTuple &tuple) { return false; }

    virtual void notifyBlockWasCompactedAway(TBPtr block) {}

    virtual void notifyTupleMovement(TBPtr sourceBlock, TBPtr targetBlock,
                                     TableTuple &sourceTuple, TableTuple &targetTuple) {
        m_test.m_shuffles.insert(*reinterpret_cast<const int64_t*>(sourceTuple.address() + 1));
    }

    virtual TableStreamerInterface* cloneForTruncatedTable(voltdb::PersistentTableSurgeon&) {
        return NULL;
    }


    CopyOnWriteTest &m_test;
    int32_t m_partitionId;
    TableStreamType m_type;
};

class ElasticTableScrambler {
public:

    ElasticTableScrambler(CopyOnWriteTest &test,
                          int npartitions, int tuplesPerBlock, int numInitial,
                          int freqInsert, int freqDelete, int freqUpdate, int freqCompaction) :
        m_test(test),
        m_npartitions(npartitions),
        m_tuplesPerBlock(tuplesPerBlock),
        m_numInitial(numInitial),
        m_freqInsert(freqInsert),
        m_freqDelete(freqDelete),
        m_freqUpdate(freqUpdate),
        m_freqCompaction(freqCompaction),
        m_icycle(0)
    {}

    void initialize() {
        m_test.initTable(m_npartitions, static_cast<int>(m_test.m_tupleWidth * (m_tuplesPerBlock + sizeof(int32_t))));

        m_test.m_table->deleteAllTuples();
        m_test.addRandomUniqueTuples(m_test.m_table, m_numInitial, &m_test.m_initial);
    }

    void scramble() {
        // Make sure to offset the initial cycles based on the frequency.
        if (m_freqInsert > 0 && (m_icycle + m_freqInsert - 1) % m_freqInsert == 0) {
            m_test.doRandomInsert(m_test.m_table, &m_test.m_inserts);
        }

        if (m_freqDelete > 0 && (m_icycle + m_freqDelete - 1) % m_freqDelete == 0) {
            m_test.doRandomDelete(m_test.m_table, &m_test.m_deletes);
        }

        if (m_freqUpdate > 0 && (m_icycle + m_freqUpdate - 1) % m_freqUpdate == 0) {
            m_test.doRandomUpdate(m_test.m_table, &m_test.m_updatesSrc, &m_test.m_updatesTgt);
        }

        if (m_freqCompaction > 0 && (m_icycle + m_freqCompaction - 1) % m_freqCompaction == 0) {
            size_t churn = m_test.m_table->activeTupleCount() / 2;
            // Delete half the tuples to create enough fragmentation for
            // compaction to happen.
            for (size_t i = 0; i < churn; i++) {
                m_test.doRandomDelete(m_test.m_table, &m_test.m_deletes);
            }
            m_test.doForcedCompaction(m_test.m_table);
            // Re-insert the same number of tuples.
            for (size_t i = 0; i < churn; i++) {
                m_test.doRandomInsert(m_test.m_table, &m_test.m_inserts);
            }
        }
        m_icycle++;
    }

    CopyOnWriteTest &m_test;

    int m_npartitions;
    int m_tuplesPerBlock;
    int m_numInitial;
    int m_freqInsert;
    int m_freqDelete;
    int m_freqUpdate;
    int m_freqCompaction;
    int m_icycle;
};

// Test the elastic scanner.
TEST_F(CopyOnWriteTest, ElasticScanner) {

    const int NUM_PARTITIONS = 1;
    const int TUPLES_PER_BLOCK = 50;
    const int NUM_INITIAL = 300;
    const int NUM_CYCLES = 300;
    const int FREQ_INSERT = 1;
    const int FREQ_DELETE = 10;
    const int FREQ_UPDATE = 5;
    const int FREQ_COMPACTION = 100;

    ElasticTableScrambler tableScrambler(*this,
                                         NUM_PARTITIONS, TUPLES_PER_BLOCK, NUM_INITIAL,
                                         FREQ_INSERT, FREQ_DELETE,
                                         FREQ_UPDATE, FREQ_COMPACTION);

    tableScrambler.initialize();

    TableTuple tuple(m_table->schema());

    DummyTableStreamer *dummyStreamer = new DummyTableStreamer(*this, 0, TABLE_STREAM_ELASTIC_INDEX);
    boost::shared_ptr<TableStreamerInterface> dummyStreamerPtr(dummyStreamer);
    boost::shared_ptr<ElasticScanner>scanner = getElasticScanner();
    std::vector<std::string> predicateStrings;
    doActivateStream(TABLE_STREAM_ELASTIC_INDEX, dummyStreamerPtr, predicateStrings, true);

    bool scanComplete = false;

    // Mutate/scan loop.
    for (size_t icycle = 0; icycle < NUM_CYCLES; icycle++) {
        // Periodically delete, insert, update, compact, etc..
        tableScrambler.scramble();

        scanComplete = !scanner->next(tuple);
        if (scanComplete) {
            break;
        }
        T_Value value = *reinterpret_cast<T_Value*>(tuple.address() + 1);
        m_returns.insert(value);
    }

    // Scan the remaining tuples that weren't encountered in the mutate/scan loop.
    if (!scanComplete) {
        while (scanner->next(tuple)) {
            T_Value value = *reinterpret_cast<T_Value*>(tuple.address() + 1);
            m_returns.insert(value);
        }
    }

    checkScanner();
}

/**
 * Dummy pass-through elastic TableStreamer for testing the index.
 */
class DummyElasticTableStreamer : public DummyTableStreamer {
public:
    DummyElasticTableStreamer(CopyOnWriteTest &test,
                              int32_t partitionId,
                              const std::vector<std::string> &predicateStrings) :
        DummyTableStreamer(test, partitionId, TABLE_STREAM_ELASTIC_INDEX),
        m_predicateStrings(predicateStrings)
    {}

    virtual bool activateStream(PersistentTableSurgeon &surgeon,
                                TableStreamType streamType,
                                const HiddenColumnFilter &filter,
                                const std::vector<std::string> &predicateStrings) {
        m_context.reset(new ElasticContext(*m_test.m_table, surgeon, m_partitionId,
                                           m_predicateStrings));
        return m_context->handleActivation(streamType) == TableStreamerContext::ACTIVATION_SUCCEEDED;
    }

    virtual int64_t streamMore(TupleOutputStreamProcessor &outputStreams,
                               TableStreamType streamType,
                               std::vector<int> &retPositions) {
        return m_context->handleStreamMore(outputStreams, retPositions);
    }

    virtual bool notifyTupleInsert(TableTuple &tuple) {
        return m_context->notifyTupleInsert(tuple);
    }

    virtual bool notifyTupleUpdate(TableTuple &tuple) {
        return m_context->notifyTupleUpdate(tuple);
    }

    virtual bool notifyTupleDelete(TableTuple &tuple) {
        return m_context->notifyTupleDelete(tuple);
    }

    virtual void notifyBlockWasCompactedAway(TBPtr block) {
        m_context->notifyBlockWasCompactedAway(block);
    }

    virtual void notifyTupleMovement(TBPtr sourceBlock, TBPtr targetBlock,
                                     TableTuple &sourceTuple, TableTuple &targetTuple) {
        DummyTableStreamer::notifyTupleMovement(sourceBlock, targetBlock, sourceTuple, targetTuple);
        m_context->notifyTupleMovement(sourceBlock, targetBlock, sourceTuple, targetTuple);
        T_Value value = *reinterpret_cast<T_Value*>(sourceTuple.address() + 1);
        m_test.m_moved.insert(value);
    }

    int32_t m_partitionId;
    const std::vector<std::string> &m_predicateStrings;
    boost::scoped_ptr<ElasticContext> m_context;
};

// Test elastic index creation.
TEST_F(CopyOnWriteTest, ElasticIndex) {
    const int NUM_PARTITIONS = 1;
    const int TUPLES_PER_BLOCK = 50;
    const int NUM_INITIAL = 300;
    const int NUM_CYCLES = 300;
    const int FREQ_INSERT = 1;
    const int FREQ_DELETE = 10;
    const int FREQ_UPDATE = 5;
    const int FREQ_COMPACTION = 100;

    ElasticTableScrambler tableScrambler(*this,
                                         NUM_PARTITIONS, TUPLES_PER_BLOCK, NUM_INITIAL,
                                         FREQ_INSERT, FREQ_DELETE,
                                         FREQ_UPDATE, FREQ_COMPACTION);

    tableScrambler.initialize();

    T_HashRangeVector ranges;
    ranges.push_back(T_HashRange(0x00000000, 0x7fffffff));
    std::vector<std::string> predicateStrings;
    predicateStrings.push_back(generateHashRangePredicate(ranges));
    std::vector<bool> deleteFlags;
    StreamPredicateList predicates;
    std::ostringstream errmsg;
    ASSERT_TRUE(predicates.parseStrings(predicateStrings, errmsg, deleteFlags));

    DummyElasticTableStreamer *streamerPtr = new DummyElasticTableStreamer(*this, 0, predicateStrings);
    boost::shared_ptr<TableStreamerInterface> streamer(streamerPtr);
    doActivateStream(TABLE_STREAM_ELASTIC_INDEX, streamer, predicateStrings, false);

    while (doStreamMore(TABLE_STREAM_ELASTIC_INDEX) != 0) {
        ;
    }

    for (size_t icycle = 0; icycle < NUM_CYCLES; icycle++) {
        tableScrambler.scramble();
    }

    checkIndex("ElasticIndex", getElasticIndex(), predicates, false);
}

/**
 * Tests that a snapshot scan and an elastic index can coexist.
 * The sequence is:
 *  1) Populate tables.
 *  2) Perform elastic index scan.
 *  3) Perform snapshot scan.
 *  4) Check the index.
 */
TEST_F(CopyOnWriteTest, SnapshotAndIndex) {
    const int NUM_PARTITIONS = 1;
    const int TUPLES_PER_BLOCK = 50;
    const int NUM_INITIAL = 300;
    const int NUM_CYCLES = 300;
    const int FREQ_INSERT = 1;
    const int FREQ_DELETE = 10;
    const int FREQ_UPDATE = 5;
    const int FREQ_COMPACTION = 100;

    // These ranges test edge conditions and also that hash range expressions
    // and elastic index predicates filter the same way.
    T_HashRangeVector testRanges;
    const int32_t maxint = std::numeric_limits<int32_t>::max();
    const int32_t minint = std::numeric_limits<int32_t>::min();
    testRanges.push_back(T_HashRange(0, maxint));
//Don't support wrapping
//    testRanges.push_back(T_HashRange(maxint, 0));
    testRanges.push_back(T_HashRange(minint, 0));
//Don't support wrapping
//    testRanges.push_back(T_HashRange(0, minint));
    testRanges.push_back(T_HashRange(minint, maxint));
//Don't support wrapping
//    testRanges.push_back(T_HashRange(maxint, minint, true));    // empty range
    testRanges.push_back(T_HashRange(-maxint/2, +maxint/2));
//Don't support wrapping
//    testRanges.push_back(T_HashRange(+maxint/2, -maxint/2));
//    testRanges.push_back(T_HashRange(0, 0));
//    testRanges.push_back(T_HashRange(maxint, maxint));

    for (int itest = 0; itest < testRanges.size(); ++itest) {
        resetTest();

        ElasticTableScrambler tableScrambler(*this,
                                             NUM_PARTITIONS, TUPLES_PER_BLOCK, NUM_INITIAL,
                                             FREQ_INSERT, FREQ_DELETE,
                                             FREQ_UPDATE, FREQ_COMPACTION);

        // Clear and populate the table.
        tableScrambler.initialize();

        // Generate separate predicates for two sequential index tests done in each iteration.
        // Using a different predicate when regenerating assures that it isn't always empty.
        T_HashRange &testRange1 = testRanges[itest];
        T_HashRange &testRange2 = testRanges[(itest + 1) % testRanges.size()];
        std::vector<std::string> predicateStrings1, predicateStrings2;
        StreamPredicateList predicates1, predicates2;
        predicateStrings1.push_back(generateHashRangePredicate(testRange1));
        predicateStrings2.push_back(generateHashRangePredicate(testRange2));
        parsePredicateList(predicateStrings1, predicates1);
        parsePredicateList(predicateStrings2, predicates2);

        // Generate the elastic index.
        streamElasticIndex(predicateStrings1, true);

        // Do some scrambling.
        for (size_t icycle = 0; icycle < NUM_CYCLES; icycle++) {
            tableScrambler.scramble();
        }

        // Stream a snapshot, mutate tuples, and check against original tuples.
        T_ValueSet originalTuples;
        getTableValueSet(originalTuples);
        T_ValueSet COWTuples;
        int totalSnapped;
        streamSnapshot(NUM_MUTATIONS, NUM_MUTATIONS, COWTuples, totalSnapped);
        checkTuples(NUM_INITIAL + (m_tuplesInserted - m_tuplesDeleted), originalTuples, COWTuples);
        ElasticIndex *directIndex = getElasticIndex();
        checkIndex(testRange1.label("direct"), directIndex, predicates1, false);
        size_t indexSizeBefore = directIndex->size();
        size_t tableSizeBefore = m_table->activeTupleCount();

        // Materialize the index and validate. Undo every other test cycle.
        ElasticIndex streamedIndex;
        size_t totalStreamed;
        bool undo = (itest % 2 == 1);
        materializeIndex(streamedIndex, testRange1, undo, totalStreamed);
        checkIndex(testRange1.label("streamed"), &streamedIndex, predicates1, true);
        size_t indexSizeAfter = directIndex->size();
        size_t tableSizeAfter = m_table->activeTupleCount();
        if (testRange1.m_empty || undo) {
            ASSERT_EQ(indexSizeAfter, indexSizeBefore);
            ASSERT_EQ(tableSizeAfter, tableSizeBefore);
        }
        else {
            ASSERT_EQ(0, indexSizeAfter);
            ASSERT_LT(tableSizeAfter, tableSizeBefore);
        }
        if (!undo) {
            ASSERT_EQ(indexSizeBefore-indexSizeAfter, tableSizeBefore-tableSizeAfter);
        }

        // Clear the index and validate.
        if (!undo) {
            clearIndex(testRange1, true);
            ASSERT_EQ(NULL, getElasticIndex());
        }
        else if (undo && itest == 1) {
            clearIndex(testRange1, false);
            ASSERT_NE(NULL, getElasticIndex());
        }

        // Also make sure we can re-stream the index.
        if (!undo) {
            ElasticIndex streamedIndex2;
            size_t totalStreamed2;
            streamElasticIndex(predicateStrings2, false);
            materializeIndex(streamedIndex2, testRange2, false, totalStreamed2);
            checkIndex(testRange2.label("streamed"), &streamedIndex2, predicates2, true);
        }

        //itest++;
    }
}

TEST_F(CopyOnWriteTest, ElasticIndexLowerUpperBounds) {
    ElasticIndex index;
    ElasticIndexKey key1(1, (char *)&index);
    bool inserted = index.add(key1);
    ASSERT_TRUE(inserted);
    ElasticIndexKey key2(3, (char *)&index);
    inserted = index.add(key2);
    ASSERT_TRUE(inserted);

    ASSERT_TRUE(key1 == *index.createLowerBoundIterator(1));
    ASSERT_TRUE(index.createUpperBoundIterator(3) == index.end());
}

// Sanity Check
// 1. Disallow Multi-Cow Activation on Same Table Streamer
// 2. Disallow Elastic_Index Activation during Snapshot
// 3. Allow Cow Activation with Elastic_Index
// 4. Allow Elastic_Index_Read / Clear with Cow
TEST_F(CopyOnWriteTest, CoexistenceCheck) {
    const int NUM_PARTITIONS = 1;
    const int TUPLES_PER_BLOCK = 50;
    const int NUM_INITIAL = 300;
    // const int NUM_CYCLES = 300;
    const int FREQ_INSERT = 1;
    const int FREQ_DELETE = 10;
    const int FREQ_UPDATE = 5;
    const int FREQ_COMPACTION = 100;

    ElasticTableScrambler tableScrambler(*this,
                                         NUM_PARTITIONS, TUPLES_PER_BLOCK, NUM_INITIAL,
                                         FREQ_INSERT, FREQ_DELETE,
                                         FREQ_UPDATE, FREQ_COMPACTION);

    tableScrambler.initialize();


    char config[4];
    ::memset(config, 0, 4);
    ReferenceSerializeInputBE input(config, 4);

    // first active elastic_index
    T_HashRangeVector ranges;
    ranges.push_back(T_HashRange(0x00000000, 0x7fffffff));
    std::vector<std::string> predicateStrings;
    predicateStrings.push_back(generateHashRangePredicate(ranges));
    boost::shared_ptr<ReferenceSerializeInputBE> predicateInput = getPredicateSerializeInput(predicateStrings);
    DummyElasticTableStreamer *streamerPtr = new DummyElasticTableStreamer(*this, 0, predicateStrings);
    boost::shared_ptr<TableStreamerInterface> streamer(streamerPtr);
    m_outputStreams.reset(new TupleOutputStreamProcessor(m_serializationBuffer, sizeof(m_serializationBuffer)));
    m_outputStream = &m_outputStreams->at(0);
    bool ok = m_table->activateStream(TABLE_STREAM_ELASTIC_INDEX, HiddenColumnFilter::NONE, 0, m_tableId, *predicateInput);
    ASSERT_TRUE(ok);

    // scan all the index
    while (doStreamMore(TABLE_STREAM_ELASTIC_INDEX) != 0) {
        ;
    }

    // try activate snapshot
    ok = m_table->activateStream(TABLE_STREAM_SNAPSHOT, HiddenColumnFilter::NONE, 0, m_tableId, input);
    ASSERT_TRUE(ok);
    // insert tuples
    int tupleCount = 4;
    addRandomUniqueTuples(m_table, tupleCount);

    // try activate another Snapshot
    ReferenceSerializeInputBE input2(config, 4);
    ok = m_table->activateStream(TABLE_STREAM_SNAPSHOT, HiddenColumnFilter::NONE, 0, m_tableId, input2);
    ASSERT_FALSE(ok);

    // try reactive elastic_index
    ok = m_table->activateStream(TABLE_STREAM_ELASTIC_INDEX, HiddenColumnFilter::NONE, 0, m_tableId, *predicateInput);
    ASSERT_FALSE(ok);

    // try materialize elastic_index
    boost::shared_ptr<ReferenceSerializeInputBE> predicateInputRead = getHashRangePredicateInput(ranges[0]);
    m_engine->setUndoToken(m_undoToken);
    ok = m_table->activateStream(TABLE_STREAM_ELASTIC_INDEX_READ, HiddenColumnFilter::NONE, 0, m_tableId, *predicateInputRead);
    ASSERT_TRUE(ok);
    while ( m_table->streamMore(*m_outputStreams, TABLE_STREAM_ELASTIC_INDEX_READ, m_retPositions) != 0) {
        ;
    }
    m_engine->releaseUndoToken(m_undoToken, false);

    // try clear elastic_index
    boost::shared_ptr<ReferenceSerializeInputBE> predicateInputClear = getHashRangePredicateInput(ranges[0]);
    ok = m_table->activateStream(TABLE_STREAM_ELASTIC_INDEX_CLEAR, HiddenColumnFilter::NONE, 0, m_tableId, *predicateInputClear);
    ASSERT_TRUE(ok);
}

int main() {
    return TestSuite::globalInstance()->runAll();
}

