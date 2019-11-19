/* This file is part of VoltDB.
 * Copyright (C) 2019 VoltDB Inc.
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

using namespace voltdb;

class SystemTableFactoryTest: public Test {
public:
    SystemTableFactoryTest() {
        srand(0);
        m_topend = new DummyTopend();
        m_pool = new Pool();
        m_quantum = createInstanceFromPool<UndoQuantum>(*m_pool, 0, m_pool);
        m_context = new ExecutorContext(0, 0, m_quantum, m_topend, m_pool, nullptr, "", 0, NULL, NULL, 0);
    }

    virtual ~SystemTableFactoryTest() {
        delete m_context;
        delete m_pool;
        delete m_topend;
        voltdb::globalDestroyOncePerProcess();
    }

protected:
    DummyTopend *m_topend;
    Pool *m_pool;
    UndoQuantum *m_quantum;
    ExecutorContext *m_context;
    SystemTableFactory m_factory;
};

TEST_F(SystemTableFactoryTest, KiplingGroup) {
    PersistentTable *table = m_factory.create(SystemTableId::KIPLING_GROUP);

    EXPECT_TRUE(table);
    EXPECT_EQ("_kipling_group", table->name());
    EXPECT_EQ(0, table->partitionColumn());
    EXPECT_EQ(5, table->schema()->columnCount());

    TableIndex *pkey = table->primaryKeyIndex();
    EXPECT_TRUE(pkey);
    EXPECT_EQ(1, pkey->getColumnIndices().size());
    delete table;
}

TEST_F(SystemTableFactoryTest, KiplingGroupMember) {
    PersistentTable *table = m_factory.create(SystemTableId::KIPLING_GROUP_MEMBER);

    EXPECT_TRUE(table);
    EXPECT_EQ("_kipling_group_member", table->name());
    EXPECT_EQ(0, table->partitionColumn());
    EXPECT_EQ(4, table->schema()->columnCount());

    TableIndex *pkey = table->primaryKeyIndex();
    EXPECT_TRUE(pkey);
    EXPECT_EQ(2, pkey->getColumnIndices().size());
    delete table;
}

TEST_F(SystemTableFactoryTest, KiplingGroupMemberProtocol) {
    PersistentTable *table = m_factory.create(SystemTableId::KIPLING_GROUP_MEMBER_PROTOCOL);

    EXPECT_TRUE(table);
    EXPECT_EQ("_kipling_group_member_protocol", table->name());
    EXPECT_EQ(0, table->partitionColumn());
    EXPECT_EQ(4, table->schema()->columnCount());

    TableIndex *pkey = table->primaryKeyIndex();
    EXPECT_TRUE(pkey);
    EXPECT_EQ(3, pkey->getColumnIndices().size());
    delete table;
}

TEST_F(SystemTableFactoryTest, KiplingGroupOffset) {
    PersistentTable *table = m_factory.create(SystemTableId::KIPLING_GROUP_OFFSET);

    EXPECT_TRUE(table);
    EXPECT_EQ("_kipling_group_offset", table->name());
    EXPECT_EQ(0, table->partitionColumn());
    EXPECT_EQ(6, table->schema()->columnCount());

    TableIndex *pkey = table->primaryKeyIndex();
    EXPECT_TRUE(pkey);
    EXPECT_EQ(3, pkey->getColumnIndices().size());
    delete table;
}

TEST_F(SystemTableFactoryTest, UnknownSystemTableId) {
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
