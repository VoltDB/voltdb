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

#include <memory>

#include "harness.h"
#include "indexes/tableindex.h"
#include "kipling/TableFactory.h"
#include "kipling/messages/JoinGroup.h"
#include "kipling/messages/OffsetCommit.h"
#include "kipling/orm/Group.h"
#include "kipling/orm/GroupOffset.h"
#include "storage/SystemTableFactory.h"

using namespace voltdb;
using namespace voltdb::kipling;

static int16_t VERSION = 3;

class GroupOrmTest: public Test, public GroupTables {
public:
    GroupOrmTest() {
        srand(0);
        m_topend = new DummyTopend();
        m_pool = new Pool();
        m_context = new ExecutorContext(0, 0, nullptr, m_topend, m_pool, nullptr, "", 0, NULL, NULL, 0);
        m_groupTable.reset(kipling::TableFactory::createGroup(m_factory));
        m_groupMemberTable.reset(kipling::TableFactory::createGroupMember(m_factory));
        m_groupMemberProtocolTable.reset(kipling::TableFactory::createGroupMemberProtocol(m_factory));
        m_groupOffsetTable.reset(kipling::TableFactory::createGroupOffset(m_factory));
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

    PersistentTable* getGroupMemberProtocolTable() const override {
        return m_groupMemberProtocolTable.get();
    }

    PersistentTable* getGroupOffsetTable() const override {
        return m_groupOffsetTable.get();
    }

protected:
    DummyTopend *m_topend;
    Pool *m_pool;
    ExecutorContext *m_context;
    SystemTableFactory m_factory;
    std::unique_ptr<PersistentTable> m_groupTable;
    std::unique_ptr<PersistentTable> m_groupMemberTable;
    std::unique_ptr<PersistentTable> m_groupMemberProtocolTable;
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

    group.initializeForInsert();
    EXPECT_TRUE(group.isDirty());

    EXPECT_FALSE(Group(*this, groupId).isInTable());

    EXPECT_EQ(0, getGroupTable()->activeTupleCount());
    group.commit(0);
    EXPECT_EQ(1, getGroupTable()->activeTupleCount());
    EXPECT_FALSE(group.isDirty());

    Group newGroup(*this, groupId);
    EXPECT_TRUE(newGroup.isInTable());
    ASSERT_EQ(group, newGroup);

    ASSERT_EQ(0, group.getGeneration());
    ASSERT_EQ(GroupState::EMPTY, group.getState());
    ASSERT_TRUE(group.getLeader().isNull());
    ASSERT_TRUE(group.getProtocol().isNull());
}

/*
 * Test updating a group
 */
TEST_F(GroupOrmTest, GroupUpdate) {
    int64_t timestamp = 1;
    NValue groupId = ValueFactory::getTempStringValue("myGroupId");
    Group group(*this, groupId);
    EXPECT_FALSE(group.isInTable());
    group.initializeForInsert();
    EXPECT_EQ(-1L, group.getCommitTimestamp());
    group.commit(++timestamp);
    EXPECT_EQ(timestamp, group.getCommitTimestamp());

    // Update the generation
    int32_t generation = group.getGeneration();
    group.incrementGeneration();
    ASSERT_EQ(generation + 1, group.getGeneration());
    ASSERT_TRUE(group.isDirty());

    group.commit(++timestamp);
    EXPECT_FALSE(group.isDirty());
    EXPECT_EQ(timestamp, group.getCommitTimestamp());

    Group lookedUp(*this, groupId);
    EXPECT_TRUE(lookedUp.isInTable());
    ASSERT_EQ(group, lookedUp);

    // Update the state
    group.setState(GroupState::STABLE);
    ASSERT_TRUE(group.isDirty());
    ASSERT_EQ(GroupState::STABLE, group.getState());
    ASSERT_NE(group, lookedUp);

    group.commit(++timestamp);
    EXPECT_FALSE(group.isDirty());
    EXPECT_EQ(timestamp, group.getCommitTimestamp());
    ASSERT_EQ(GroupState::STABLE, group.getState());
    ASSERT_EQ(group, lookedUp);

    // Update the leader
    NValue leader = ValueFactory::getTempStringValue("leaderID");
    group.setLeader(leader);
    ASSERT_TRUE(group.isDirty());
    ASSERT_EQ(leader, group.getLeader());
    ASSERT_NE(group, lookedUp);

    group.commit(++timestamp);
    EXPECT_FALSE(group.isDirty());
    EXPECT_EQ(timestamp, group.getCommitTimestamp());
    ASSERT_EQ(leader, group.getLeader());
    ASSERT_EQ(group, lookedUp);

    // Update the protocol
    NValue protocol = ValueFactory::getTempStringValue("MyProtocol");
    group.setProtocol(protocol);
    ASSERT_TRUE(group.isDirty());
    ASSERT_EQ(protocol, group.getProtocol());
    ASSERT_NE(group, lookedUp);

    group.commit(++timestamp);
    EXPECT_FALSE(group.isDirty());
    EXPECT_EQ(timestamp, group.getCommitTimestamp());
    ASSERT_EQ(protocol, group.getProtocol());
    ASSERT_EQ(group, lookedUp);
}

/*
 * Test deleting a group
 */
TEST_F(GroupOrmTest, GroupDelete) {
    NValue groupId = ValueFactory::getTempStringValue("myGroupId");
    Group group(*this, groupId);
    group.initializeForInsert();
    group.commit(0);
    EXPECT_FALSE(group.isDirty());

    group.markForDelete();
    EXPECT_TRUE(group.isDirty());
    EXPECT_TRUE(group.isDeleted());

    group.commit(0);
    Group lookedUp(*this, groupId);
    EXPECT_FALSE(lookedUp.isInTable());
    EXPECT_EQ(0, getGroupTable()->activeTupleCount());
}

/*
 * Test adding a new group members
 */
TEST_F(GroupOrmTest, AddMembers) {
    NValue groupId = ValueFactory::getTempStringValue("myGroupId");

    Group group(*this, groupId);
    group.initializeForInsert();
    group.commit(0);

    NValue bogusMemberId = ValueFactory::getTempStringValue("abcdefaadsfadsf");
    EXPECT_FALSE(group.getMember(bogusMemberId));
    EXPECT_EQ(0, group.getMembers().size());

    std::vector<GroupMember*> members;
    // Add first member
    {
        GroupMember &member = group.getOrCreateMember(bogusMemberId);
        members.push_back(&member);
        EXPECT_EQ(1, group.getMembers().size());
        EXPECT_FALSE(member.isInTable());
        EXPECT_TRUE(member.isDirty());

        int32_t sessionTimeout = 5000, rebalacneTimeout = 10000;
        NValue instanceId = ValueFactory::getTempStringValue("myInstanceId"), assignments =
                ValueFactory::getTempBinaryValue("123456789", 9);

        std::vector<JoinGroupProtocol> protocols;
        JoinGroupRequest request(VERSION, groupId, ValueFactory::getNullStringValue(), sessionTimeout,
                rebalacneTimeout, instanceId, protocols);

        ASSERT_NE(rebalacneTimeout, member.getRebalanceTimeout());
        ASSERT_NE(sessionTimeout, member.getSessionTimeout());
        ASSERT_NE(instanceId, member.getInstanceId());
        member.update(request);
        ASSERT_EQ(rebalacneTimeout, member.getRebalanceTimeout());
        ASSERT_EQ(sessionTimeout, member.getSessionTimeout());
        ASSERT_EQ(instanceId, member.getInstanceId());

        ASSERT_NE(assignments, member.getAssignments());
        member.setAssignments(assignments);
        ASSERT_EQ(assignments, member.getAssignments());

        GroupMember *memberPointer = group.getMember(member.getMemberId());
        EXPECT_TRUE(memberPointer);
        EXPECT_EQ(member, *memberPointer);

        // Test lookup before commit
        {
            Group newGroup(*this, groupId);
            for (auto member : members) {
                EXPECT_FALSE(newGroup.getMember(member->getMemberId()));
            }
            EXPECT_EQ(0, newGroup.getMembers().size());
        }

        EXPECT_EQ(0, getGroupMemberTable()->activeTupleCount());
        group.commit(0);
        EXPECT_EQ(1, getGroupMemberTable()->activeTupleCount());
        EXPECT_FALSE(member.isDirty());

        // Test lookup after commit
        {
            Group newGroup(*this, groupId);
            for (auto member : members) {
                memberPointer = newGroup.getMember(member->getMemberId());
                EXPECT_TRUE(memberPointer);
                EXPECT_FALSE(memberPointer->isDirty());
                EXPECT_EQ(*member, *memberPointer);
            }
            EXPECT_EQ(members.size(), newGroup.getMembers().size());
        }
    }

    // Add second member
    {
        GroupMember &member = group.getOrCreateMember(ValueFactory::getNullStringValue());
        EXPECT_TRUE(member.isDirty());
        members.push_back(&member);
        EXPECT_NE(members[0]->getMemberId(), member.getMemberId());
        EXPECT_EQ(2, group.getMembers().size());

        EXPECT_EQ(1, getGroupMemberTable()->activeTupleCount());
        group.commit(0);
        EXPECT_EQ(2, getGroupMemberTable()->activeTupleCount());
        EXPECT_FALSE(member.isDirty());

        {
            Group newGroup(*this, groupId);
            for (auto member : members) {
                GroupMember *memberPointer = newGroup.getMember(member->getMemberId());
                EXPECT_TRUE(memberPointer);
                EXPECT_FALSE(memberPointer->isDirty());
                EXPECT_EQ(*member, *memberPointer);
            }
            EXPECT_EQ(members.size(), newGroup.getMembers().size());
        }
    }
}

/*
 * Test that updating group member behaves correctly
 */
TEST_F(GroupOrmTest, UpdateMembers) {
    NValue groupId = ValueFactory::getTempStringValue("myGroupId");

    Group group(*this, groupId);
    group.initializeForInsert();
    GroupMember& member1 = group.getOrCreateMember(ValueFactory::getNullStringValue());
    GroupMember& member2 = group.getOrCreateMember(ValueFactory::getNullStringValue());
    EXPECT_NE(member1, member2);
    group.commit(0);

    EXPECT_FALSE(member1.isDirty());
    EXPECT_FALSE(member2.isDirty());

    std::vector<JoinGroupProtocol> protocols;
    {
        JoinGroupRequest request1(3, groupId, ValueFactory::getNullStringValue(), 10000, member1.getRebalanceTimeout(),
                member2.getInstanceId(), protocols);
        JoinGroupRequest request2(3, groupId, ValueFactory::getNullStringValue(), member2.getSessionTimeout(), 5000,
                        member2.getInstanceId(), protocols);
        member1.update(request1);
        member2.update(request2);
    }

    EXPECT_TRUE(member1.isDirty());
    EXPECT_TRUE(member2.isDirty());

    // members looked up are not equal before commit
    {
        Group newGroup(*this, groupId);
        EXPECT_NE(member1, *newGroup.getMember(member1.getMemberId()));
        EXPECT_NE(member2, *newGroup.getMember(member2.getMemberId()));
    }

    group.commit(0);

    EXPECT_FALSE(member1.isDirty());
    EXPECT_FALSE(member2.isDirty());

    // members looked up are equal after commit
    {
        Group newGroup(*this, groupId);
        EXPECT_EQ(member1, *newGroup.getMember(member1.getMemberId()));
        EXPECT_EQ(member2, *newGroup.getMember(member2.getMemberId()));
    }

    {
        JoinGroupRequest request1(3, groupId, ValueFactory::getNullStringValue(), member1.getSessionTimeout(),
                member1.getRebalanceTimeout(), ValueFactory::getTempStringValue("instanceId"),
                protocols);
        member1.update(request1);

        char assignmentBytes[32] = { -42 };
        NValue assignments = ValueFactory::getTempBinaryValue(assignmentBytes, sizeof(assignmentBytes));
        member2.setAssignments(assignments);
        EXPECT_EQ(assignments, member2.getAssignments());
    }

    // members looked up are not equal before commit
    {
        Group newGroup(*this, groupId);
        EXPECT_NE(member1, *newGroup.getMember(member1.getMemberId()));
        EXPECT_NE(member2, *newGroup.getMember(member2.getMemberId()));
    }

    group.commit(0);

    // members looked up are equal after commit
    {
        Group newGroup(*this, groupId);
        EXPECT_EQ(member1, *newGroup.getMember(member1.getMemberId()));
        EXPECT_EQ(member2, *newGroup.getMember(member2.getMemberId()));
    }
}

/*
 * Test that deleting group members and groups behaves correctly
 */
TEST_F(GroupOrmTest, DeleteMembers) {
    NValue groupId = ValueFactory::getTempStringValue("myGroupId");

    Group group(*this, groupId);
    group.initializeForInsert();
    GroupMember& member1 = group.getOrCreateMember(ValueFactory::getNullStringValue());
    GroupMember& member2 = group.getOrCreateMember(ValueFactory::getNullStringValue());
    group.commit(0);

    EXPECT_EQ(2, getGroupMemberTable()->activeTupleCount());

    member1.markForDelete();
    ASSERT_TRUE(member1.isDirty());
    ASSERT_FALSE(member2.isDirty());
    ASSERT_TRUE(member1.isDeleted());

    EXPECT_EQ(1, group.getMembers().size());
    EXPECT_EQ(2, group.getMembers(true).size());

    // deleted member looked up are not equal before commit
    {
        Group newGroup(*this, groupId);
        EXPECT_NE(member1, *newGroup.getMember(member1.getMemberId()));
        EXPECT_EQ(member2, *newGroup.getMember(member2.getMemberId()));
    }

    group.commit(0);

    EXPECT_EQ(1, getGroupMemberTable()->activeTupleCount());

    ASSERT_FALSE(member1.isDirty());
    ASSERT_FALSE(member2.isDirty());

    // deleted member should not exist after commit
    {
        Group newGroup(*this, groupId);
        EXPECT_FALSE(newGroup.getMember(member1.getMemberId()));
        EXPECT_EQ(member2, *newGroup.getMember(member2.getMemberId()));
    }

    // Deleting the group should delete all members
    group.markForDelete();
    ASSERT_FALSE(member1.isDirty());
    ASSERT_TRUE(member2.isDirty());

    group.commit(0);

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
 * Test that the initial cration of protocols works correctly
 */
TEST_F(GroupOrmTest, InsertProtocols) {
    NValue groupId = ValueFactory::getTempStringValue("myGroupId");

    Group group(*this, groupId);
    group.initializeForInsert();
    GroupMember& member1 = group.getOrCreateMember(ValueFactory::getNullStringValue());

    char metadata1[25] = { 48 };
    char metadata2[85] = { 91 };

    std::vector<JoinGroupProtocol> protocols = {
            JoinGroupProtocol(VERSION, ValueFactory::getTempStringValue("proto1"),
                    ValueFactory::getTempBinaryValue(metadata1, sizeof(metadata1))),
            JoinGroupProtocol(VERSION, ValueFactory::getTempStringValue("proto2"),
                    ValueFactory::getTempBinaryValue(metadata2, sizeof(metadata2)))};
    JoinGroupRequest request(VERSION, groupId, member1.getMemberId(), 5000, 10000,
            ValueFactory::getNullStringValue(), protocols);

    EXPECT_EQ(0, member1.getProtocols().size());
    member1.update(request);
    EXPECT_EQ(2, member1.getProtocols().size());

    for (auto protocol : member1.getProtocols()) {
        EXPECT_TRUE(protocol->isDirty());
        EXPECT_FALSE(protocol->isInTable());
    }

    for (int i = 0; i < protocols.size(); ++i) {
        GroupMemberProtocol* protocol = member1.getProtocol(protocols[i].name());
        EXPECT_TRUE(protocol);
        EXPECT_EQ(i, protocol->getIndex());
        EXPECT_EQ(protocols[i].name(), protocol->getName());
        EXPECT_EQ(protocols[i].metadata(), protocol->getMetadata());
    }

    EXPECT_EQ(0, getGroupMemberProtocolTable()->activeTupleCount());
    group.commit(0);
    EXPECT_EQ(2, getGroupMemberProtocolTable()->activeTupleCount());

    for (auto protocol : member1.getProtocols()) {
        EXPECT_FALSE(protocol->isDirty());
        EXPECT_TRUE(protocol->isInTable());
    }

    Group newGroup(*this, groupId);
    GroupMember* newMember = newGroup.getMember(member1.getMemberId());
    EXPECT_TRUE(newMember);
    EXPECT_EQ(2, newMember->getProtocols().size());
    for (auto protocol : member1.getProtocols()) {
        EXPECT_EQ(*protocol, *newMember->getProtocol(protocol->getName()));
    }
}

/*
 * Test that protocol updates including deletes works correctly
 */
TEST_F(GroupOrmTest, UpdateProtocols) {
    NValue groupId = ValueFactory::getTempStringValue("myGroupId");

    Group group(*this, groupId);
    group.initializeForInsert();
    GroupMember& member1 = group.getOrCreateMember(ValueFactory::getNullStringValue());

    char metadata1[25] = { 48 };
    char metadata2[85] = { 91 };

    std::vector<JoinGroupProtocol> protocols = {
            JoinGroupProtocol(VERSION, ValueFactory::getTempStringValue("proto1"),
                    ValueFactory::getTempBinaryValue(metadata1, sizeof(metadata1))),
            JoinGroupProtocol(VERSION, ValueFactory::getTempStringValue("proto2"),
                    ValueFactory::getTempBinaryValue(metadata2, sizeof(metadata2)))};
    JoinGroupRequest request(VERSION, groupId, member1.getMemberId(), 5000, 10000,
            ValueFactory::getNullStringValue(), protocols);

    member1.update(request);
    group.commit(0);

    // No changes should not cause any update
    {
        JoinGroupRequest update(VERSION, groupId, member1.getMemberId(), member1.getSessionTimeout(),
                member1.getRebalanceTimeout(), member1.getInstanceId(), protocols);
        EXPECT_FALSE(member1.update(update));
        for (auto protocol : member1.getProtocols()) {
            EXPECT_FALSE(protocol->isDirty());
        }
    }

    // Add one protocol
    {
        char metadata3[64] = { -58 };
        std::vector<JoinGroupProtocol> protocolUpdates = {
                protocols[0], protocols[1],
                JoinGroupProtocol(VERSION, ValueFactory::getTempStringValue("proto3"),
                                    ValueFactory::getTempBinaryValue(metadata3, sizeof(metadata3)))};
        JoinGroupRequest update(VERSION, groupId, member1.getMemberId(), member1.getSessionTimeout(),
                member1.getRebalanceTimeout(), member1.getInstanceId(), protocolUpdates);

        EXPECT_TRUE(member1.update(update));

        EXPECT_EQ(3, member1.getProtocols().size());
    }

    // Before commit the looked up group should only have 2 protocols
    {
        Group newGroup(*this, groupId);
        GroupMember* newMember = newGroup.getMember(member1.getMemberId());
        EXPECT_TRUE(newMember);
        EXPECT_EQ(2, newMember->getProtocols().size());
    }

    group.commit(0);

    // After commit the looked up group should have 3 protocols
    {
        Group newGroup(*this, groupId);
        GroupMember* newMember = newGroup.getMember(member1.getMemberId());
        EXPECT_TRUE(newMember);
        EXPECT_EQ(3, newMember->getProtocols().size());
        for (auto protocol : member1.getProtocols()) {
            EXPECT_EQ(*protocol, *newMember->getProtocol(protocol->getName()));
        }
    }

    // Delete one protocol and create a new one
    {
        char metadata4[189] = { 111 };
        std::vector<JoinGroupProtocol> protocolUpdates = {
                protocols[0], protocols[1],
                JoinGroupProtocol(VERSION, ValueFactory::getTempStringValue("proto4"),
                                    ValueFactory::getTempBinaryValue(metadata4, sizeof(metadata4)))};
        JoinGroupRequest update(VERSION, groupId, member1.getMemberId(), member1.getSessionTimeout(),
                member1.getRebalanceTimeout(), member1.getInstanceId(), protocolUpdates);

        EXPECT_TRUE(member1.update(update));

        EXPECT_EQ(4, member1.getProtocols(true).size());

        EXPECT_TRUE(
                member1.getProtocol(ValueFactory::getTempStringValue("proto3"))->isDeleted());
    }

    group.commit(0);

    // After commit the looked up group should have 3 protocols and no deleted
    {
        Group newGroup(*this, groupId);
        GroupMember* newMember = newGroup.getMember(member1.getMemberId());
        EXPECT_TRUE(newMember);
        EXPECT_EQ(3, newMember->getProtocols(true).size());
        for (auto protocol : member1.getProtocols()) {
            EXPECT_EQ(*protocol, *newMember->getProtocol(protocol->getName()));
        }
    }

    // Change the order of 2 protocols and update the metadata of the third
    {
        char metadata4[68] = { 71 };

        std::vector<JoinGroupProtocol> protocolUpdates = {
                protocols[1], protocols[0],
                JoinGroupProtocol(VERSION, ValueFactory::getTempStringValue("proto4"),
                                    ValueFactory::getTempBinaryValue(metadata4, sizeof(metadata4)))};
        JoinGroupRequest update(VERSION, groupId, member1.getMemberId(), member1.getSessionTimeout(),
                member1.getRebalanceTimeout(), member1.getInstanceId(), protocolUpdates);

        EXPECT_TRUE(member1.update(update));
        for (auto protocol : member1.getProtocols()) {
            EXPECT_TRUE(protocol->isDirty());
        }
        EXPECT_EQ(3, member1.getProtocols().size());
        EXPECT_EQ(1, member1.getProtocol(protocols[0].name())->getIndex());
        EXPECT_EQ(0, member1.getProtocol(protocols[1].name())->getIndex());
        EXPECT_EQ(protocolUpdates[2].metadata(),
                member1.getProtocol(protocolUpdates[2].name())->getMetadata());
    }

    group.commit(0);

    // After commit the looked up group should have 3 protocols with the updated indexes and metadata
    {
        Group newGroup(*this, groupId);
        GroupMember* newMember = newGroup.getMember(member1.getMemberId());
        EXPECT_TRUE(newMember);
        EXPECT_EQ(3, newMember->getProtocols(true).size());
        for (auto protocol : member1.getProtocols()) {
            EXPECT_EQ(*protocol, *newMember->getProtocol(protocol->getName()));
        }
    }

    // Deleting member should delete all protocols
    member1.markForDelete();
    for (auto protocol : member1.getProtocols()) {
        EXPECT_TRUE(protocol->isDirty());
        EXPECT_TRUE(protocol->isDeleted());
    }

    group.commit(0);

    EXPECT_EQ(0, getGroupMemberProtocolTable()->activeTupleCount());
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

    OffsetCommitRequestPartition request(VERSION, partition, 15, 1, ValueFactory::getTempStringValue("my metadata"));
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
    OffsetCommitRequestPartition request(VERSION, partition, 15, 1, ValueFactory::getTempStringValue("my metadata"));
    offset.update(request);
    offset.commit(0);

    // Update the offset value
    {
        OffsetCommitRequestPartition update(VERSION, partition, offset.getOffset() + 1, offset.getLeaderEpoch(),
                offset.getMetadata());
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
        OffsetCommitRequestPartition update(VERSION, partition, offset.getOffset(), offset.getLeaderEpoch() + 1,
                offset.getMetadata());
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
        OffsetCommitRequestPartition update(VERSION, partition, offset.getOffset(), offset.getLeaderEpoch(),
                ValueFactory::getTempStringValue("different metadata"));
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
    OffsetCommitRequestPartition request1(VERSION, partition1, 15, 1, ValueFactory::getTempStringValue("my metadata"));
    offset1.update(request1);
    offset1.commit(0);

    // Create a second offset
    GroupOffset offset2(*this, groupId, topic, partition2);
    OffsetCommitRequestPartition request2(VERSION, partition2, 15, 1, ValueFactory::getTempStringValue("my metadata"));
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

int main() {
    return TestSuite::globalInstance()->runAll();
}
