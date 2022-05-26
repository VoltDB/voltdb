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

#include <memory>

#include "harness.h"
#include "topics/GroupTestBase.h"
#include "indexes/tableindex.h"
#include "topics/TableFactory.h"
#include "topics/messages/OffsetCommit.h"
#include "topics/orm/Group.h"
#include "topics/orm/GroupOffset.h"
#include "storage/SystemTableFactory.h"

using namespace voltdb;
using namespace voltdb::topics;

class GroupOrmTest: public GroupTestBase, public GroupTables {
public:
    GroupOrmTest() {
        srand(0);
        m_topend = new DummyTopend();
        m_pool = new Pool();
        m_context = new ExecutorContext(0, 0, nullptr, m_topend, m_pool, nullptr, "", 0, NULL, NULL, 0);
        m_groupTable.reset(topics::TableFactory::createGroup(m_factory));
        m_groupMemberTable.reset(topics::TableFactory::createGroupMember(m_factory));
        m_groupOffsetTable.reset(topics::TableFactory::createGroupOffset(m_factory));
    }

    virtual ~GroupOrmTest() {
        delete m_context;
        delete m_pool;
        delete m_topend;
        voltdb::globalDestroyOncePerProcess();
    }

    PersistentTable* getGroupTable() const override {
        return m_groupTable.get();
    }

    PersistentTable* getGroupMemberTable() const override {
        return m_groupMemberTable.get();
    }

    PersistentTable* getGroupOffsetTable() const override {
        return m_groupOffsetTable.get();
    }

protected:
    void upsertGroup(const NValue& groupId, int64_t timestamp, int32_t generationId, const NValue& leader,
            const NValue& protocol) {
        Group update(*this, groupId, timestamp, generationId, leader, protocol);

        ASSERT_EQ(groupId, update.getGroupId());
        ASSERT_EQ(timestamp, update.getCommitTimestamp());
        ASSERT_EQ(generationId, update.getGeneration());
        ASSERT_EQ(leader, update.getLeader());
        ASSERT_EQ(protocol, update.getProtocol());

        upsertGroup(update);
    }

    void upsertGroup(Group& update) {
        char scratch[1024];
        ReferenceSerializeOutput out(scratch, sizeof(scratch));
        update.serialize(out);
        ASSERT_EQ(out.position(), update.serializedSize());

        ReferenceSerializeInputBE in(scratch, sizeof(scratch));
        Group::upsert(*this, in);

        validateGroupCommited(*this, update);
    }

    DummyTopend *m_topend;
    Pool *m_pool;
    ExecutorContext *m_context;
    SystemTableFactory m_factory;
    std::unique_ptr<PersistentTable> m_groupTable;
    std::unique_ptr<PersistentTable> m_groupMemberTable;
    std::unique_ptr<PersistentTable> m_groupOffsetTable;
};

/*
 * Test creating a new group
 */
TEST_F(GroupOrmTest, GroupInsert) {
    NValue groupId = ValueFactory::getTempStringValue("myGroupId");

    Group group(*this, groupId);
    EXPECT_FALSE(group.isInTable());
    EXPECT_FALSE(group.isDirty());

    NValue leader = ValueFactory::getTempStringValue("leader");
    NValue protocol = ValueFactory::getTempStringValue("protocol");

    EXPECT_EQ(0, getGroupTable()->activeTupleCount());

    upsertGroup(groupId, 1, 2, leader, protocol);

    EXPECT_EQ(1, getGroupTable()->activeTupleCount());
    ASSERT_EQ(0, getGroupTable()->index(GroupTable::standaloneGroupIndexName)->getSize());
}

TEST_F(GroupOrmTest, GroupStandaloneInsert) {
    NValue groupId = ValueFactory::getTempStringValue("myGroupId");

    Group group(*this, groupId);
    EXPECT_FALSE(group.isInTable());
    EXPECT_FALSE(group.isDirty());

    EXPECT_EQ(0, getGroupTable()->activeTupleCount());

    // For standalone group use an empty protocol (not null)
    upsertGroup(groupId, 1, 2, ValueFactory::getNullStringValue(), ValueFactory::getTempStringValue(""));
    EXPECT_EQ(1, getGroupTable()->activeTupleCount());
    ASSERT_EQ(1, getGroupTable()->index(GroupTable::standaloneGroupIndexName)->getSize());

    int count = 0;
    Group::visitStandaloneGroups(*this, [this, groupId, &count] (const NValue &actualGroupId) mutable {
        ASSERT_EQ(groupId, actualGroupId);
        ++count;
    });
    ASSERT_EQ(1, count);
}

/*
 * Test updating a group
 */
TEST_F(GroupOrmTest, GroupUpdate) {
    NValue groupId = ValueFactory::getTempStringValue("myGroupId");
    Group group(*this, groupId);
    EXPECT_FALSE(group.isInTable());

    int64_t timestamp = 1;
    int32_t generationId = 2;
    NValue leader = ValueFactory::getTempStringValue("leader");
    NValue protocol = ValueFactory::getTempStringValue("protocol");

    upsertGroup(groupId, timestamp, generationId, leader, protocol);
    EXPECT_EQ(1, getGroupTable()->activeTupleCount());

    // Update the timestamp
    timestamp += 10;
    upsertGroup(groupId, timestamp , generationId, leader, protocol);
    EXPECT_EQ(1, getGroupTable()->activeTupleCount());

    // Update the generation
    generationId += 55;
    upsertGroup(groupId, timestamp, generationId, leader, protocol);
    EXPECT_EQ(1, getGroupTable()->activeTupleCount());

    // Update the leader
    leader = ValueFactory::getTempStringValue("leaderID");
    upsertGroup(groupId, timestamp, generationId, leader, protocol);
    EXPECT_EQ(1, getGroupTable()->activeTupleCount());

    // Update the protocol
    protocol = ValueFactory::getTempStringValue("MyProtocol");
    upsertGroup(groupId, timestamp, generationId, leader, protocol);
    EXPECT_EQ(1, getGroupTable()->activeTupleCount());

    // Convert to standalone group and back
    EXPECT_EQ(0, getGroupTable()->index(GroupTable::standaloneGroupIndexName)->getSize());
    upsertGroup(groupId, timestamp, generationId, leader, ValueFactory::getTempStringValue(""));
    EXPECT_EQ(1, getGroupTable()->activeTupleCount());
    EXPECT_EQ(1, getGroupTable()->index(GroupTable::standaloneGroupIndexName)->getSize());

    upsertGroup(groupId, timestamp, generationId, leader, protocol);
    EXPECT_EQ(1, getGroupTable()->activeTupleCount());
    EXPECT_EQ(0, getGroupTable()->index(GroupTable::standaloneGroupIndexName)->getSize());
}

/*
 * Test deleting a group
 */
TEST_F(GroupOrmTest, GroupDelete) {
    NValue groupId1 = ValueFactory::getTempStringValue("myGroupId1");
    NValue groupId2 = ValueFactory::getTempStringValue("myGroupId2");
    NValue leader = ValueFactory::getTempStringValue("leader");
    NValue protocol = ValueFactory::getTempStringValue("protocol");

    EXPECT_EQ(0, getGroupTable()->activeTupleCount());

    upsertGroup(groupId1, 1, 2, leader, protocol);
    // Create standalone group
    upsertGroup(groupId2, 1, 2, ValueFactory::getNullStringValue(), ValueFactory::getTempStringValue(""));

    // Delete standalone group
    EXPECT_EQ(1, getGroupTable()->index(GroupTable::standaloneGroupIndexName)->getSize());
    Group group2(*this, groupId2);
    group2.markForDelete();
    EXPECT_TRUE(group2.isDirty());
    EXPECT_TRUE(group2.isDeleted());

    EXPECT_EQ(2, getGroupTable()->activeTupleCount());
    group2.commit();
    EXPECT_EQ(1, getGroupTable()->activeTupleCount());
    EXPECT_EQ(0, getGroupTable()->index(GroupTable::standaloneGroupIndexName)->getSize());

    {
        Group lookedUp(*this, groupId2);
        EXPECT_FALSE(lookedUp.isInTable());
    }

    // Delete regular group
    Group group1(*this, groupId1);
    group1.markForDelete();
    EXPECT_TRUE(group1.isDirty());
    EXPECT_TRUE(group1.isDeleted());

    EXPECT_EQ(1, getGroupTable()->activeTupleCount());
    group1.commit();
    EXPECT_EQ(0, getGroupTable()->activeTupleCount());

    {
        Group lookedUp(*this, groupId1);
        EXPECT_FALSE(lookedUp.isInTable());
    }
}

/*
 * Test adding a new group members
 */
TEST_F(GroupOrmTest, AddMembers) {
    NValue groupId = ValueFactory::getTempStringValue("myGroupId");
    NValue leader = ValueFactory::getTempStringValue("leader");
    NValue protocol = ValueFactory::getTempStringValue("protocol");

    upsertGroup(groupId, 1, 2, leader, protocol);

    {
        Group group(*this, groupId);

        NValue bogusMemberId = ValueFactory::getTempStringValue("abcdefaadsfadsf");
        EXPECT_FALSE(group.getMember(bogusMemberId));
        EXPECT_EQ(0, group.getMembers().size());
    }

    EXPECT_EQ(0, getGroupMemberTable()->activeTupleCount());
    // Add first member
    {
        RandomData scratch(128);
        Group group(*this, groupId);
        group.getOrCreateMember(generateGroupMemberid()).update(ValueFactory::getTempStringValue("truc"),
                ValueFactory::getTempStringValue("bidule"), 1000, 2000, ValueFactory::getNullStringValue(),
                ValueFactory::getTempBinaryValue(&scratch, 64), ValueFactory::getTempBinaryValue(&scratch[64], 64));
        upsertGroup(group);
        EXPECT_EQ(1, getGroupMemberTable()->activeTupleCount());
    }

    // Add second member
    {
        RandomData scratch(128);
        NValue instanceId = ValueFactory::getTempStringValue("instanceId");
        Group group(*this, groupId);
        group.getOrCreateMember(generateGroupMemberid()).update(ValueFactory::getTempStringValue("truc"),
                ValueFactory::getTempStringValue("bidule"), 1000, 2000, instanceId,
                ValueFactory::getTempBinaryValue(&scratch, 64), ValueFactory::getTempBinaryValue(&scratch[64], 64));
        upsertGroup(group);
        EXPECT_EQ(2, getGroupMemberTable()->activeTupleCount());
    }
}

/*
 * Test that updating group member behaves correctly
 */
TEST_F(GroupOrmTest, UpdateMembers) {
    NValue groupId = ValueFactory::getTempStringValue("myGroupId");
    NValue leader = ValueFactory::getTempStringValue("leader");
    NValue protocol = ValueFactory::getTempStringValue("protocol");
    NValue instanceId = ValueFactory::getTempStringValue("instanceId");

    Group group(*this, groupId, 1, 2, leader, protocol);
    RandomData scratch(128);
    GroupMember& member1 = group.getOrCreateMember(generateGroupMemberid());
    member1.update(ValueFactory::getTempStringValue("truc"),
            ValueFactory::getTempStringValue("bidule"), 1000, 2000, ValueFactory::getNullStringValue(), ValueFactory::getTempBinaryValue(&scratch, 64),
            ValueFactory::getTempBinaryValue(&scratch[64], 64));
    GroupMember &member2 = group.getOrCreateMember(generateGroupMemberid());
    member2.update(ValueFactory::getTempStringValue("truc"),
            ValueFactory::getTempStringValue("bidule"), 1000, 2000, instanceId, ValueFactory::getTempBinaryValue(&scratch, 64),
            ValueFactory::getTempBinaryValue(&scratch[64], 64));
    EXPECT_NE(member1, member2);

    upsertGroup(group);

    // Update timeouts
    {
        member1.update(ValueFactory::getTempStringValue("truc"),
                ValueFactory::getTempStringValue("bidule"), 5000, 2000, ValueFactory::getNullStringValue(),
                ValueFactory::getTempBinaryValue(&scratch, 64),
                ValueFactory::getTempBinaryValue(&scratch[64], 64));
        member2.update(ValueFactory::getTempStringValue("truc"),
                ValueFactory::getTempStringValue("bidule"), 1000, 10000, instanceId,
                ValueFactory::getTempBinaryValue(&scratch, 64),
                ValueFactory::getTempBinaryValue(&scratch[64], 64));
    }

    // members looked up are not equal before commit
    {
        Group newGroup(*this, groupId);
        ASSERT_NE(member1.getSessionTimeout(), newGroup.getMember(member1.getMemberId())->getSessionTimeout());
        ASSERT_NE(member2.getRebalanceTimeout(), newGroup.getMember(member2.getMemberId())->getRebalanceTimeout());
    }

    upsertGroup(group);

    // Update protocol metadata and assignments
    {
        RandomData scratch2(128);
        member1.update(ValueFactory::getTempStringValue("truc"),
                ValueFactory::getTempStringValue("bidule"), 5000, 2000, ValueFactory::getNullStringValue(),
                ValueFactory::getTempBinaryValue(&scratch2, 128),
                ValueFactory::getTempBinaryValue(&scratch[64], 64));
        member2.update(ValueFactory::getTempStringValue("truc"),
                ValueFactory::getTempStringValue("bidule"), 1000, 10000, instanceId,
                ValueFactory::getTempBinaryValue(&scratch, 64),
                ValueFactory::getTempBinaryValue(&scratch2, 128));
    }

    // members looked up are not equal before commit
    {
        Group newGroup(*this, groupId);
        ASSERT_NE(member1.getProtocolMetadata(), newGroup.getMember(member1.getMemberId())->getProtocolMetadata());
        ASSERT_NE(member2.getAssignments(), newGroup.getMember(member2.getMemberId())->getAssignments());
    }

    upsertGroup(group);
}

/*
 * Test that deleting group members and groups behaves correctly
 */
TEST_F(GroupOrmTest, DeleteMembers) {
    NValue groupId = ValueFactory::getTempStringValue("myGroupId");
    NValue leader = ValueFactory::getTempStringValue("leader");
    NValue protocol = ValueFactory::getTempStringValue("protocol");
    NValue instanceId = ValueFactory::getTempStringValue("instanceId");

    Group group(*this, groupId, 1, 2, leader, protocol);
    RandomData scratch(128);
    GroupMember& member1 = group.getOrCreateMember(generateGroupMemberid());
    member1.update(ValueFactory::getTempStringValue("truc"), ValueFactory::getTempStringValue("bidule"),
            1000, 2000, ValueFactory::getNullStringValue(), ValueFactory::getTempBinaryValue(&scratch, 64),
            ValueFactory::getTempBinaryValue(&scratch[64], 64));
    GroupMember &member2 = group.getOrCreateMember(generateGroupMemberid());
    member2.update(ValueFactory::getTempStringValue("truc"), ValueFactory::getTempStringValue("bidule"),
            1000, 2000, instanceId, ValueFactory::getTempBinaryValue(&scratch, 64),
            ValueFactory::getTempBinaryValue(&scratch[64], 64));

    upsertGroup(group);


    EXPECT_EQ(2, getGroupMemberTable()->activeTupleCount());

    member1.markForDelete();
    ASSERT_TRUE(member1.isDeleted());

    EXPECT_EQ(1, group.getMembers().size());
    EXPECT_EQ(2, group.getMembers(true).size());

    // deleted member looked up are not equal before commit
    {
        Group newGroup(*this, groupId);
        EXPECT_TRUE(newGroup.getMember(member1.getMemberId())->isInTable());
        EXPECT_TRUE(newGroup.getMember(member2.getMemberId())->isInTable());
    }

    upsertGroup(group);

    EXPECT_EQ(1, getGroupMemberTable()->activeTupleCount());

    // deleted member should not exist after commit
    {
        Group newGroup(*this, groupId);
        EXPECT_FALSE(newGroup.getMember(member1.getMemberId()));
        EXPECT_TRUE(newGroup.getMember(member2.getMemberId()));
    }

    // Deleting the group should delete all members
    {
        Group newGroup(*this, groupId);
        newGroup.markForDelete();
        newGroup.commit();
    }

    EXPECT_EQ(0, getGroupTable()->activeTupleCount());
    EXPECT_EQ(0, getGroupMemberTable()->activeTupleCount());

    // deleted members should not exist after commit
    {
        Group newGroup(*this, groupId);
        EXPECT_FALSE(newGroup.isInTable());
        EXPECT_FALSE(newGroup.getMember(member1.getMemberId()));
        EXPECT_FALSE(newGroup.getMember(member2.getMemberId()));
    }
}

/*
 * Test inserting an offset in the table
 */
TEST_F(GroupOrmTest, InsertOffset) {
    NValue groupId = ValueFactory::getTempStringValue("myGroupId");
    NValue topic = ValueFactory::getTempStringValue("myTopic");
    int partition = 5;

    GroupOffset offset(*this, groupId, topic, partition);
    EXPECT_FALSE(offset.isInTable());
    EXPECT_FALSE(offset.isDirty());
    EXPECT_EQ(groupId, offset.getGroupId());
    EXPECT_EQ(topic, offset.getTopic());
    EXPECT_EQ(partition, offset.getPartition());

    OffsetCommitRequestPartition request(partition, 15, 1, "my metadata");
    offset.update(request);
    EXPECT_TRUE(offset.isDirty());
    EXPECT_EQ(request.offset(), offset.getOffset());
    EXPECT_EQ(request.leaderEpoch(), offset.getLeaderEpoch());
    EXPECT_EQ(request.metadata(), offset.getMetadata());

    // Offset should not be in table yet
    EXPECT_EQ(0, getGroupOffsetTable()->activeTupleCount());
    {
        GroupOffset newOffset(*this, groupId, topic, partition);
        EXPECT_FALSE(newOffset.isInTable());
    }

    offset.commit(0);
    EXPECT_FALSE(offset.isDirty());

    // Offset should now be in table
    EXPECT_EQ(1, getGroupOffsetTable()->activeTupleCount());
    {
        GroupOffset newOffset(*this, groupId, topic, partition);
        EXPECT_TRUE(newOffset.isInTable());
        EXPECT_EQ(newOffset, newOffset);
    }

    // offsets with different topics or partitions should not be in the table
    {
        GroupOffset newOffset(*this, groupId, topic, 6);
        EXPECT_FALSE(newOffset.isInTable());
    }
    {
        GroupOffset newOffset(*this, groupId, ValueFactory::getTempStringValue("other"), partition);
        EXPECT_FALSE(newOffset.isInTable());
    }
}

/*
 * Test updating offsets in the table
 */
TEST_F(GroupOrmTest, UpdateOffset) {
    NValue groupId = ValueFactory::getTempStringValue("myGroupId");
    NValue topic = ValueFactory::getTempStringValue("myTopic");
    int partition = 5;

    GroupOffset offset(*this, groupId, topic, partition);
    std::string metadata = "my metadata";
    OffsetCommitRequestPartition request(partition, 15, 1, metadata);
    offset.update(request);
    offset.commit(0);

    // Update the offset value
    {
        OffsetCommitRequestPartition update(partition, offset.getOffset() + 1, offset.getLeaderEpoch(),
                metadata);
        offset.update(update);

        EXPECT_EQ(update.offset(), offset.getOffset());

        GroupOffset newOffset(*this, groupId, topic, partition);
        EXPECT_NE(offset, newOffset);
    }

    EXPECT_TRUE(offset.isDirty());
    offset.commit(0);
    EXPECT_FALSE(offset.isDirty());

    {
        GroupOffset newOffset(*this, groupId, topic, partition);
        EXPECT_EQ(offset, newOffset);
    }

    // Update the leader epoch value
    {
        OffsetCommitRequestPartition update(partition, offset.getOffset(), offset.getLeaderEpoch() + 1,
                metadata);
        offset.update(update);

        EXPECT_EQ(update.leaderEpoch(), offset.getLeaderEpoch());

        GroupOffset newOffset(*this, groupId, topic, partition);
        EXPECT_NE(offset, newOffset);
    }

    EXPECT_TRUE(offset.isDirty());
    offset.commit(0);
    EXPECT_FALSE(offset.isDirty());

    {
        GroupOffset newOffset(*this, groupId, topic, partition);
        EXPECT_EQ(offset, newOffset);
    }

    // Update the metadata value
    {
        OffsetCommitRequestPartition update(partition, offset.getOffset(), offset.getLeaderEpoch(),
                "different metadata");
        offset.update(update);

        EXPECT_EQ(update.metadata(), offset.getMetadata());

        GroupOffset newOffset(*this, groupId, topic, partition);
        EXPECT_NE(offset, newOffset);
    }

    EXPECT_TRUE(offset.isDirty());
    offset.commit(0);
    EXPECT_FALSE(offset.isDirty());

    {
        GroupOffset newOffset(*this, groupId, topic, partition);
        EXPECT_EQ(offset, newOffset);
    }
}

/*
 * Test deleting offsets from the table
 */
TEST_F(GroupOrmTest, DeleteOffset) {
    NValue groupId = ValueFactory::getTempStringValue("myGroupId");
    NValue topic = ValueFactory::getTempStringValue("myTopic");
    int partition1 = 5, partition2 = 19;

    GroupOffset offset1(*this, groupId, topic, partition1);
    OffsetCommitRequestPartition request1(partition1, 15, 1, "my metadata");
    offset1.update(request1);
    offset1.commit(0);

    // Create a second offset
    GroupOffset offset2(*this, groupId, topic, partition2);
    OffsetCommitRequestPartition request2(partition2, 15, 1, "my metadata");
    offset2.update(request2);
    offset2.commit(0);

    EXPECT_EQ(2, getGroupOffsetTable()->activeTupleCount());

    offset1.markForDelete();
    EXPECT_TRUE(offset1.isDeleted());
    EXPECT_TRUE(offset1.isDirty());

    // Should still find offset in table before commit
    {
        GroupOffset newOffset(*this, groupId, topic, partition1);
        EXPECT_TRUE(newOffset.isInTable());
        EXPECT_NE(offset1, newOffset);
    }

    offset1.commit(0);
    EXPECT_TRUE(offset1.isDeleted());
    EXPECT_FALSE(offset1.isDirty());

    // offset should not be in table anymore
    EXPECT_EQ(1, getGroupOffsetTable()->activeTupleCount());
    {
        GroupOffset newOffset(*this, groupId, topic, partition1);
        EXPECT_FALSE(newOffset.isInTable());
        EXPECT_EQ(offset1, newOffset);
    }

    // offset 2 should still be in the table
    {
        GroupOffset newOffset(*this, groupId, topic, partition2);
        EXPECT_TRUE(newOffset.isInTable());
        EXPECT_EQ(offset2, newOffset);
    }
}

/*
 * Test that GroupOffset::visitAll will visit all offsets for the given groupID
 */
TEST_F(GroupOrmTest, VisitAllOffsets) {
    NValue groupIdBefore = ValueFactory::getTempStringValue("abb");
    NValue groupId =  ValueFactory::getTempStringValue("abc");
    NValue groupIdAfter = ValueFactory::getTempStringValue("abd");

    NValue topic = ValueFactory::getTempStringValue("topic");

    for (auto group : { groupIdBefore, groupId, groupIdAfter }) {
        for (auto partition : {3, 6, 19, 25}) {
            OffsetCommitRequestPartition request(partition, 15, 1, "metadata");
            GroupOffset offset(*this, group, topic, partition);
            offset.update(request);
            offset.commit(0);
        }
    }

    int offsetCount = 0;
    GroupOffset::visitAll(*this, groupId, [&groupId, &topic, &offsetCount, this] (const GroupOffset& offset) {
        ASSERT_EQ(groupId, offset.getGroupId());
        ASSERT_EQ(topic, offset.getTopic());
        ASSERT_EQ(15, offset.getOffset());
        ++offsetCount;
    });

    ASSERT_EQ(4, offsetCount);
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
