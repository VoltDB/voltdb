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
#include "common/common.h"
#include "common/NValue.hpp"
#include "common/ValueFactory.hpp"
#include "common/tabletuple.h"
#include "indexes/tableindex.h"
#include "indexes/indexkey.h"
#include "indexes/CompactingTreeMultiMapIndex.h"
#include "indexes/tableindexfactory.h"
#include <ctime>
#include <cstring>

using namespace std;
using namespace voltdb;

class CompactingTreeMultiIndexTest : public Test {
public:
    CompactingTreeMultiIndexTest() {}
    TableTuple *newTuple(TupleSchema *schema, int idx, long value) {
        TableTuple *tuple = new TableTuple(schema);
        char *data = new char[tuple->tupleLength()];
        memset(data, 0, tuple->tupleLength());
        tuple->move(data);

        tuple->setNValue(idx, ValueFactory::getBigIntValue(value));
        return tuple;
    }

    char* initTuples(TupleSchema *schema, int places) {
        long num = 1L << places;
        char *data = new char[25 * num];
        if (data == NULL)
            return NULL;
        memset(data, 0, 25 * num);
        for (long ii = 0; ii < num; ii++) {
            TableTuple tempTuple(data + (25 * ii), schema);
            tempTuple.setNValue(0, ValueFactory::getBigIntValue(12345));
            tempTuple.setNValue(1, ValueFactory::getBigIntValue(45688));
            tempTuple.setNValue(2, ValueFactory::getBigIntValue(rand()));
        }
        return data;
    }

    std::clock_t insertTuplesIntoIndex(TableIndex *index, TupleSchema *schema, char *data, int places) {
        long limit = 1L << places;
        TableTuple tempTuple(data, schema);
        std::clock_t start = std::clock();
        for (long ii = 0; ii < limit; ii++) {
            tempTuple.move(data + (25 * ii));
            index->addEntry(&tempTuple);
        }
        std::clock_t end = std::clock();
        return end - start;
    }

    std::clock_t insertTuplesIntoIndex2(TableIndex *index, TupleSchema *schema, char *data, int places) {
        long limit = 1L << places;
        long tmp = 1L << (places/2);
        TableTuple tempTuple(data, schema);
        std::clock_t start = std::clock();
        for (long ii = 0; ii < limit; ii++) {
            long jj = ((ii % tmp) << (places/2)) + (ii / tmp);
            tempTuple.move(data + (25 * jj));
            index->addEntry(&tempTuple);
        }
        std::clock_t end = std::clock();
        return end - start;
    }

    // delete num tuples
    std::clock_t deleteTuplesFromIndex(TableIndex *index, TupleSchema *schema, char *data, int places, int num) {
        EXPECT_EQ(index->getSize(), (1L << places));
        long gap = (1L << places) / num;
        TableTuple deleteTuple(data, schema);
        std::clock_t start = std::clock();
        for (int ii = 0; ii < num; ii++) {
            deleteTuple.move(data + (25 * gap * ii));
            index->deleteEntry(&deleteTuple);
        }
        std::clock_t end = std::clock();
        // check correctness of delete
        for (int ii = 0; ii < num; ii++) {
            deleteTuple.move(data + (25 * gap * ii));
            EXPECT_FALSE(index->exists(&deleteTuple));
        }
        EXPECT_EQ(index->getSize(), ((1L << places) - num));
        return end - start;
    }

    // init all the vectors, and we don't clear the vectors now
    void prepareForPerformanceDifference() {
        // tuple schema
        for(int i = 0; i < 3; i++) {
            m_columnTypes.push_back(VALUE_TYPE_BIGINT);
            m_columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_BIGINT));
            m_columnAllowNull.push_back(false);
        }

        m_columnIndices.push_back(0);
        // index using one column
        m_kcolumnTypes.push_back(VALUE_TYPE_BIGINT);
        m_kcolumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_BIGINT));
        m_kcolumnAllowNull.push_back(false);

        // index using two columns
        m_columnIndices2.push_back(0);
        m_kcolumnTypes2.push_back(VALUE_TYPE_BIGINT);
        m_kcolumnLengths2.push_back(NValue::getTupleStorageSize(VALUE_TYPE_BIGINT));
        m_kcolumnAllowNull2.push_back(false);
        m_columnIndices2.push_back(1);
        m_kcolumnTypes2.push_back(VALUE_TYPE_BIGINT);
        m_kcolumnLengths2.push_back(NValue::getTupleStorageSize(VALUE_TYPE_BIGINT));
        m_kcolumnAllowNull2.push_back(false);
    }

    void createSchemaAndIndexForPerformanceDifference() {
        m_schema = TupleSchema::createTupleSchemaForTest(m_columnTypes,
                                                         m_columnLengths,
                                                         m_columnAllowNull);
        m_schema1 = TupleSchema::createTupleSchemaForTest(m_columnTypes,
                                                          m_columnLengths,
                                                          m_columnAllowNull);
        m_schema2 = TupleSchema::createTupleSchemaForTest(m_columnTypes,
                                                          m_columnLengths,
                                                          m_columnAllowNull);
        m_kschema1 = TupleSchema::createTupleSchemaForTest(m_kcolumnTypes,
                                                           m_kcolumnLengths,
                                                           m_kcolumnAllowNull);
        m_kschema2 = TupleSchema::createTupleSchemaForTest(m_kcolumnTypes2,
                                                           m_kcolumnLengths2,
                                                           m_kcolumnAllowNull2);
        assert(m_schema);
        assert(m_schema1);
        assert(m_schema2);
        assert(m_kschema1);
        assert(m_kschema2);
        TableIndexScheme scheme("test_index", BALANCED_TREE_INDEX,
                                m_columnIndices, TableIndex::simplyIndexColumns(),
                                false, false, m_schema);
        TableIndexScheme scheme1("test_index1", BALANCED_TREE_INDEX,
                                m_columnIndices, TableIndex::simplyIndexColumns(),
                                false, false, m_schema1);
        TableIndexScheme scheme2("test_index2", BALANCED_TREE_INDEX,
                                m_columnIndices2, TableIndex::simplyIndexColumns(),
                                false, false, m_schema2);
        // build index
        // index has one column and pointer
        m_index = TableIndexFactory::getInstance(scheme);
        // index has one column
        m_indexWithoutPointer1 =
            new CompactingTreeMultiMapIndex<NormalKeyValuePair<IntsKey<1> >, false>(m_kschema1, scheme1);
        // index has two columns
        m_indexWithoutPointer2 =
            new CompactingTreeMultiMapIndex<NormalKeyValuePair<IntsKey<2> >, false>(m_kschema2, scheme2);
        assert(m_index);
        assert(m_indexWithoutPointer1);
        assert(m_indexWithoutPointer2);
    }

    void freeSchemaAndIndexForPerformanceDifference() {
        TupleSchema::freeTupleSchema(m_schema);
        TupleSchema::freeTupleSchema(m_schema1);
        TupleSchema::freeTupleSchema(m_schema2);
        m_schema = m_schema1 = m_schema2 = NULL;
        delete m_index;
        delete m_indexWithoutPointer1;
        delete m_indexWithoutPointer2;
        m_index = m_indexWithoutPointer1 = m_indexWithoutPointer2 = NULL;
    }

    // for tuple schema
    vector<ValueType> m_columnTypes;
    vector<int32_t> m_columnLengths;
    vector<bool> m_columnAllowNull;

    // for key schema
    vector<int> m_columnIndices, m_columnIndices2;
    vector<ValueType> m_kcolumnTypes, m_kcolumnTypes2;
    vector<int32_t> m_kcolumnLengths, m_kcolumnLengths2;
    vector<bool> m_kcolumnAllowNull, m_kcolumnAllowNull2;

    TupleSchema *m_schema, *m_schema1, *m_schema2;
    TupleSchema *m_kschema1, *m_kschema2;
    TableIndex *m_index, *m_indexWithoutPointer1, *m_indexWithoutPointer2;
};

TEST_F(CompactingTreeMultiIndexTest, SimpleDeleteTuple) {
    TableIndex *index = NULL;
    vector<int> columnIndices;
    vector<ValueType> columnTypes;
    vector<int32_t> columnLengths;
    vector<bool> columnAllowNull;

    columnIndices.push_back(0);
    columnTypes.push_back(VALUE_TYPE_BIGINT);
    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_BIGINT));
    columnAllowNull.push_back(false);

    TupleSchema *schema = TupleSchema::createTupleSchemaForTest(columnTypes,
                                                         columnLengths,
                                                         columnAllowNull);

    TableIndexScheme scheme("test_index", BALANCED_TREE_INDEX,
                            columnIndices, TableIndex::simplyIndexColumns(),
                            false, false, schema);
    index = TableIndexFactory::getInstance(scheme);

    TableTuple *tuple1 = newTuple(schema, 0, 10);
    index->addEntry(tuple1);
    TableTuple *tuple2 = newTuple(schema, 0, 11);
    index->addEntry(tuple2);
    TableTuple *tuple3 = newTuple(schema, 0, 12);
    index->addEntry(tuple3);

    TableTuple *tuple4 = newTuple(schema, 0, 10);
    EXPECT_TRUE(index->replaceEntryNoKeyChange(*tuple4, *tuple1));

    EXPECT_FALSE(index->exists(tuple1));
    EXPECT_TRUE(index->exists(tuple2));
    EXPECT_TRUE(index->exists(tuple3));
    EXPECT_TRUE(index->exists(tuple4));

    delete index;
    TupleSchema::freeTupleSchema(schema);
    delete[] tuple1->address();
    delete tuple1;
    delete[] tuple2->address();
    delete tuple2;
    delete[] tuple3->address();
    delete tuple3;
    delete[] tuple4->address();
    delete tuple4;
}

// create three types of index and test their performace of delete
TEST_F(CompactingTreeMultiIndexTest, PerformanceDifference) {
    std::cout<<std::endl;
    prepareForPerformanceDifference();

#define PLACES 16
    for (int places = PLACES; places >= 4; places -= 2 ) {
        createSchemaAndIndexForPerformanceDifference();
        char *data = initTuples(m_schema, places);
        EXPECT_NE(data, NULL);

        std::clock_t c1 = insertTuplesIntoIndex(m_index, m_schema, data, places);
        std::cout<<"insert 2**"<<places<< " IntsPointerKey<1> : "<< c1 <<std::endl;
        std::clock_t c2 = insertTuplesIntoIndex(m_indexWithoutPointer1, m_schema, data, places);
        std::cout<<"insert 2**"<<places<< " IntsKey<1> : "<< c2 <<std::endl;
        std::clock_t c3 = insertTuplesIntoIndex(m_indexWithoutPointer2, m_schema, data, places);
        std::cout<<"insert 2**"<<places<< " IntsKey<2> : "<< c3 <<std::endl;

        c1 = deleteTuplesFromIndex(m_index, m_schema, data, places, 7);
        std::cout<<"delete 2**"<<places<< " IntsPointerKey<1> : "<< c1 <<std::endl;
        c2 = deleteTuplesFromIndex(m_indexWithoutPointer1, m_schema, data, places, 7);
        std::cout<<"delete 2**"<<places<< " IntsKey<1> : "<< c2 <<std::endl;
        c3 = deleteTuplesFromIndex(m_indexWithoutPointer2, m_schema, data, places, 7);
        std::cout<<"delete 2**"<<places<< " IntsKey<2> : "<< c3 <<std::endl;

        delete data;
        freeSchemaAndIndexForPerformanceDifference();
    }

    for (int places = PLACES; places >= 4; places -= 2 ) {
        createSchemaAndIndexForPerformanceDifference();
        char *data = initTuples(m_schema, places);
        EXPECT_NE(data, NULL);

        std::clock_t c1 = insertTuplesIntoIndex2(m_index, m_schema, data, places);
        std::cout<<"insert 2**"<<places<< " IntsPointerKey<1> : "<< c1 <<std::endl;
        std::clock_t c2 = insertTuplesIntoIndex2(m_indexWithoutPointer1, m_schema, data, places);
        std::cout<<"insert 2**"<<places<< " IntsKey<1> : "<< c2 <<std::endl;
        std::clock_t c3 = insertTuplesIntoIndex2(m_indexWithoutPointer2, m_schema, data, places);
        std::cout<<"insert 2**"<<places<< " IntsKey<2> : "<< c3 <<std::endl;

        c1 = deleteTuplesFromIndex(m_index, m_schema, data, places, 7);
        std::cout<<"delete 2**"<<places<< " IntsPointerKey<1> : "<< c1 <<std::endl;
        c2 = deleteTuplesFromIndex(m_indexWithoutPointer1, m_schema, data, places, 7);
        std::cout<<"delete 2**"<<places<< " IntsKey<1> : "<< c2 <<std::endl;
        c3 = deleteTuplesFromIndex(m_indexWithoutPointer2, m_schema, data, places, 7);
        std::cout<<"delete 2**"<<places<< " IntsKey<2> : "<< c3 <<std::endl;

        delete data;
        freeSchemaAndIndexForPerformanceDifference();
    }
}

int main()
{
    return TestSuite::globalInstance()->runAll();
}
