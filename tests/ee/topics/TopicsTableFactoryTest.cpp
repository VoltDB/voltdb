/* This file is part of VoltDB.
 * Copyright (C) 2019-2022 Volt Active Data Inc.
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
#include "storage/SystemTableFactory.h"
#include "indexes/tableindex.h"
#include "topics/TableFactory.h"

using namespace voltdb;
using namespace voltdb::topics;

class TopicsTableFactoryTest: public Test {
public:
    TopicsTableFactoryTest() {
        srand(0);
        m_topend = new voltdb::DummyTopend();
        m_pool = new voltdb::Pool();
        m_context = new ExecutorContext(0, 0, nullptr, m_topend, m_pool, nullptr, "", 0, NULL, NULL, 0);
    }

    virtual ~TopicsTableFactoryTest() {
        delete m_context;
        delete m_pool;
        delete m_topend;
        voltdb::globalDestroyOncePerProcess();
    }

protected:
    template<typename Column>
    ValueType columnType(const TupleSchema* schema, Column column) {
        return schema->columnType(static_cast<int>(column));
    }

    DummyTopend *m_topend;
    Pool *m_pool;
    ExecutorContext *m_context;
    SystemTableFactory m_factory;
};

TEST_F(TopicsTableFactoryTest, TopicsGroup) {
    PersistentTable *table = m_factory.create(SystemTableId::TOPICS_GROUP);

    EXPECT_TRUE(table);
    EXPECT_EQ(GroupTable::name, table->name());
    EXPECT_EQ(0, table->partitionColumn());
    EXPECT_EQ(5, table->schema()->columnCount());

    const TupleSchema* schema = table->schema();
    EXPECT_EQ(ValueType::tVARCHAR, columnType(schema, GroupTable::Column::ID));
    EXPECT_EQ(ValueType::tTIMESTAMP, columnType(schema, GroupTable::Column::COMMIT_TIMESTAMP));
    EXPECT_EQ(ValueType::tINTEGER, columnType(schema, GroupTable::Column::GENERATION));
    EXPECT_EQ(ValueType::tVARCHAR, columnType(schema, GroupTable::Column::LEADER));
    EXPECT_EQ(ValueType::tVARCHAR, columnType(schema, GroupTable::Column::PROTOCOL));

    TableIndex *index = table->index(GroupTable::pkIndexName);
    EXPECT_TRUE(index);
    EXPECT_EQ(index, table->primaryKeyIndex());
    EXPECT_EQ(1, index->getColumnIndices().size());
    EXPECT_EQ(static_cast<int>(GroupTable::Column::ID), index->getColumnIndices()[0]);

    index = table->index(GroupTable::standaloneGroupIndexName);
    EXPECT_TRUE(index);
    EXPECT_NE(index, table->primaryKeyIndex());
    EXPECT_EQ(1, index->getColumnIndices().size());
    EXPECT_EQ(static_cast<int>(GroupTable::Column::ID), index->getColumnIndices()[0]);

    delete table;
}

TEST_F(TopicsTableFactoryTest, TopicsGroupMember) {
    PersistentTable *table = m_factory.create(SystemTableId::TOPICS_GROUP_MEMBER);

    EXPECT_TRUE(table);
    EXPECT_EQ(GroupMemberTable::name, table->name());
    EXPECT_EQ(0, table->partitionColumn());
    EXPECT_EQ(9, table->schema()->columnCount());

    const TupleSchema* schema = table->schema();
    EXPECT_EQ(ValueType::tVARCHAR, columnType(schema, GroupMemberTable::Column::GROUP_ID));
    EXPECT_EQ(ValueType::tVARCHAR, columnType(schema, GroupMemberTable::Column::MEMBER_ID));
    EXPECT_EQ(ValueType::tVARCHAR, columnType(schema, GroupMemberTable::Column::CLIENT_ID));
    EXPECT_EQ(ValueType::tVARCHAR, columnType(schema, GroupMemberTable::Column::CLIENT_HOST));
    EXPECT_EQ(ValueType::tINTEGER, columnType(schema, GroupMemberTable::Column::SESSION_TIMEOUT));
    EXPECT_EQ(ValueType::tINTEGER, columnType(schema, GroupMemberTable::Column::REBALANCE_TIMEOUT));
    EXPECT_EQ(ValueType::tVARCHAR, columnType(schema, GroupMemberTable::Column::INSTANCE_ID));
    EXPECT_EQ(ValueType::tVARBINARY, columnType(schema, GroupMemberTable::Column::PROTOCOL_METADATA));
    EXPECT_EQ(ValueType::tVARBINARY, columnType(schema, GroupMemberTable::Column::ASSIGNMENTS));

    TableIndex *index = table->index(GroupMemberTable::indexName);
    EXPECT_TRUE(index);
    EXPECT_FALSE(table->primaryKeyIndex());
    EXPECT_EQ(1, index->getColumnIndices().size());
    EXPECT_EQ(static_cast<int>(GroupMemberTable::Column::GROUP_ID), index->getColumnIndices()[0]);

    delete table;
}

TEST_F(TopicsTableFactoryTest, TopicsGroupOffset) {
    PersistentTable *table = m_factory.create(SystemTableId::TOPICS_GROUP_OFFSET);

    EXPECT_TRUE(table);
    EXPECT_EQ(GroupOffsetTable::name, table->name());
    EXPECT_EQ(0, table->partitionColumn());
    EXPECT_EQ(7, table->schema()->columnCount());

    const TupleSchema* schema = table->schema();
    EXPECT_EQ(ValueType::tVARCHAR, columnType(schema, GroupOffsetTable::Column::GROUP_ID));
    EXPECT_EQ(ValueType::tVARCHAR, columnType(schema, GroupOffsetTable::Column::TOPIC));
    EXPECT_EQ(ValueType::tINTEGER, columnType(schema, GroupOffsetTable::Column::PARTITION));
    EXPECT_EQ(ValueType::tBIGINT, columnType(schema, GroupOffsetTable::Column::COMMITTED_OFFSET));
    EXPECT_EQ(ValueType::tINTEGER, columnType(schema, GroupOffsetTable::Column::LEADER_EPOCH));
    EXPECT_EQ(ValueType::tVARCHAR, columnType(schema, GroupOffsetTable::Column::METADATA));
    EXPECT_EQ(ValueType::tTIMESTAMP, columnType(schema, GroupOffsetTable::Column::COMMIT_TIMESTAMP));

    TableIndex *index = table->index(GroupOffsetTable::indexName);
    EXPECT_TRUE(index);
    EXPECT_EQ(index, table->primaryKeyIndex());
    EXPECT_EQ(3, index->getColumnIndices().size());
    EXPECT_EQ(static_cast<int>(GroupOffsetTable::Column::GROUP_ID), index->getColumnIndices()[0]);
    EXPECT_EQ(static_cast<int>(GroupOffsetTable::Column::TOPIC), index->getColumnIndices()[1]);
    EXPECT_EQ(static_cast<int>(GroupOffsetTable::Column::PARTITION), index->getColumnIndices()[2]);

    delete table;
}

TEST_F(TopicsTableFactoryTest, UnknownSystemTableId) {
    try {
        m_factory.create(static_cast<SystemTableId>(0));
        FAIL("Should have thrown an exception");
    } catch (const SerializableEEException &e) {
        EXPECT_EQ(VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_GENERIC, e.getType());
    }
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
