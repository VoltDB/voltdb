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

#include "topics/GroupStore.h"
#include "topics/GroupTestBase.h"
#include "common/executorcontext.hpp"
#include "storage/SystemTableFactory.h"
#include "topics/messages/OffsetCommit.h"
#include "topics/messages/OffsetFetch.h"
#include "topics/orm/Group.h"
#include "topics/orm/GroupOffset.h"

using namespace voltdb;
using namespace voltdb::topics;

class GroupStoreTest: public GroupTestBase {
public:
    using CommitOffsets = std::unordered_map<NValue, std::vector<OffsetCommitRequestPartition>>;
    using FetchOffsets = std::unordered_map<NValue, std::vector<int32_t>>;

    GroupStoreTest() {
        srand(0);
        m_topend = new DummyTopend();
        m_pool = new Pool();
        m_context = new ExecutorContext(0, 0, nullptr, m_topend, m_pool, nullptr, "", 0, NULL, NULL, 0);
        m_groupStore.initialize(topics::TableFactory::createGroup(m_factory),
                topics::TableFactory::createGroupMember(m_factory),
                topics::TableFactory::createGroupOffset(m_factory));
    }

    virtual ~GroupStoreTest() {
        delete m_context;
        delete m_pool;
        delete m_topend;
        voltdb::globalDestroyOncePerProcess();
    }

    void upsertGroup(Group& update) {
        char scratch[1024];
        ReferenceSerializeOutput out(scratch, sizeof(scratch));
        update.serialize(out);

        ReferenceSerializeInputBE in(scratch, sizeof(scratch));
        m_groupStore.storeGroup(in);

        validateGroupCommited(m_groupStore, update);
    }

    void commitOffsets(const NValue& groupId, const CommitOffsets& offsets, int64_t timestamp = 0L) {
        char scratch[1024];
        {
            ReferenceSerializeOutput out(scratch, sizeof(scratch));
            out.writeInt(offsets.size());
            for (auto entry : offsets) {
                ResponseComponent::writeString(entry.first, out);

                out.writeInt(entry.second.size());
                for (auto offset : entry.second) {
                    out.writeInt(offset.partitionIndex());
                    out.writeLong(offset.offset());
                    out.writeInt(offset.leaderEpoch());
                    ResponseComponent::writeString(offset.metadata(), out);
                }
            }
        }

        // It is ok if in == out since the write occurs after the complete read
        ReferenceSerializeInputBE in(scratch, sizeof(scratch));
        ReferenceSerializeOutput out(scratch, sizeof(scratch));
        m_groupStore.commitOffsets(timestamp, static_cast<int16_t>(7), groupId, in, out);

        for (auto entry : offsets) {
            for (auto partition : entry.second) {
                GroupOffset offset(m_groupStore, groupId, entry.first, partition.partitionIndex());
                ASSERT_TRUE(offset.isInTable());
                ASSERT_EQ(partition.offset(), offset.getOffset());
                ASSERT_EQ(partition.leaderEpoch(), offset.getLeaderEpoch());
                ASSERT_EQ(partition.metadata(), offset.getMetadata());
            }
        }
    }

    OffsetFetchResponse fetchOffsets(const NValue& groupId, FetchOffsets& offsets) {
        int16_t version = 5;
        char scratch[1024];
        {
            ReferenceSerializeOutput out(scratch, sizeof(scratch));
            out.writeInt(offsets.size());
            for (auto entry : offsets) {
                ResponseComponent::writeString(entry.first, out);

                out.writeInt(entry.second.size());
                for (auto partition : entry.second) {
                    out.writeInt(partition);
                }
            }
        }

        {
            ReferenceSerializeInputBE in(scratch, sizeof(scratch));
            ReferenceSerializeOutput out(scratch, sizeof(scratch));
            m_groupStore.fetchOffsets(version, groupId, in, out);
        }

        {
            ReferenceSerializeInputBE inTemp(scratch, sizeof(scratch));
            int32_t actualLength = inTemp.readInt();
            EXPECT_TRUE(actualLength < sizeof(scratch) - sizeof(int32_t));
            ReferenceSerializeInputBE in(&scratch[sizeof(int32_t)], actualLength);
            CheckedSerializeInput checkedIn(in);
            OffsetFetchResponse response(version, checkedIn);
            EXPECT_EQ(0, in.remaining());
            return response;
        }
    }

protected:
    DummyTopend *m_topend;
    Pool *m_pool;
    ExecutorContext *m_context;
    SystemTableFactory m_factory;
    GroupStore m_groupStore;
};

/*
 * Test the groups can be stored and updated
 */
TEST_F(GroupStoreTest, StoreGroup) {
    NValue groupId = ValueFactory::getTempStringValue("groupId");
    NValue leader = ValueFactory::getTempStringValue("leader");
    NValue protocol = ValueFactory::getTempStringValue("protocol");

    Group group(m_groupStore, groupId, 1000, 5, leader, protocol);

    // Insert group
    {
        RandomData scratch(128);
        group.getOrCreateMember(generateGroupMemberid()).update(ValueFactory::getTempStringValue("truc"),
                ValueFactory::getTempStringValue("bidule"), 1000, 2000, ValueFactory::getNullStringValue(),
                ValueFactory::getTempBinaryValue(&scratch, 64), ValueFactory::getTempBinaryValue(&scratch[64], 64));

        ASSERT_EQ(0, m_groupStore.getGroupTable()->activeTupleCount());
        ASSERT_EQ(0, m_groupStore.getGroupMemberTable()->activeTupleCount());

        upsertGroup(group);

        ASSERT_EQ(1, m_groupStore.getGroupTable()->activeTupleCount());
        ASSERT_EQ(1, m_groupStore.getGroupMemberTable()->activeTupleCount());
    }

    // Update group with one more member
    {
        RandomData scratch(128);

        group.getOrCreateMember(generateGroupMemberid()).update(ValueFactory::getTempStringValue("truc"),
                ValueFactory::getTempStringValue("bidule"), 1000, 2000, ValueFactory::getNullStringValue(),
                ValueFactory::getTempBinaryValue(&scratch, 64), ValueFactory::getTempBinaryValue(&scratch[64], 64));

        upsertGroup(group);

        ASSERT_EQ(1, m_groupStore.getGroupTable()->activeTupleCount());
        ASSERT_EQ(2, m_groupStore.getGroupMemberTable()->activeTupleCount());
    }

    // Update group removing one member
    {
        group.getMembers()[0]->markForDelete();

        upsertGroup(group);

        ASSERT_EQ(1, m_groupStore.getGroupTable()->activeTupleCount());
        ASSERT_EQ(1, m_groupStore.getGroupMemberTable()->activeTupleCount());
    }

    // Add a second group
    {
        NValue groupId2 = ValueFactory::getTempStringValue("groupId2");
        Group group2(m_groupStore, groupId2, 1000, 5, leader, protocol);

        RandomData scratch(128);
        group2.getOrCreateMember(generateGroupMemberid()).update(ValueFactory::getTempStringValue("truc"),
                ValueFactory::getTempStringValue("bidule"), 1000, 2000, ValueFactory::getNullStringValue(),
                ValueFactory::getTempBinaryValue(&scratch, 64), ValueFactory::getTempBinaryValue(&scratch[64], 64));
        group2.getOrCreateMember(generateGroupMemberid()).update(ValueFactory::getTempStringValue("truc"),
                ValueFactory::getTempStringValue("bidule"), 1000, 2000, ValueFactory::getNullStringValue(),
                ValueFactory::getTempBinaryValue(&scratch, 64), ValueFactory::getTempBinaryValue(&scratch[64], 64));

        upsertGroup(group2);

        ASSERT_EQ(2, m_groupStore.getGroupTable()->activeTupleCount());
        ASSERT_EQ(3, m_groupStore.getGroupMemberTable()->activeTupleCount());
    }
}

/*
 * Test that offsets can be committed
 */
TEST_F(GroupStoreTest, CommitOffsets) {
    NValue groupId = ValueFactory::getTempStringValue("groupId");
    CommitOffsets offsets;

    {
        Group group(m_groupStore, groupId, 1000, 5, ValueFactory::getNullStringValue(), ValueFactory::getTempStringValue("protocol"));
        upsertGroup(group);
    }

    std::vector<NValue> topics = { ValueFactory::getTempStringValue("topic1"), ValueFactory::getTempStringValue(
            "topic2"), ValueFactory::getTempStringValue("topic3") };
    {
        std::vector<OffsetCommitRequestPartition> partitions = {
                OffsetCommitRequestPartition(1, 200, 0, ""),
                OffsetCommitRequestPartition(2, 500, 5, "mine"),
                OffsetCommitRequestPartition(3, 600, -1, "something")};
        for (auto topic : topics) {
            offsets.emplace(topic, partitions);
        }
    }

    ASSERT_EQ(0, m_groupStore.getGroupOffsetTable()->activeTupleCount());
    commitOffsets(groupId, offsets);
    ASSERT_EQ(9, m_groupStore.getGroupOffsetTable()->activeTupleCount());

    {
        std::vector<OffsetCommitRequestPartition> partitions = { OffsetCommitRequestPartition(1, 600, 1, ""),
                OffsetCommitRequestPartition(5, 50, 5, "other") };
        for (auto topic : topics) {
            offsets.erase(topic);
            offsets.emplace(topic, partitions);
        }
    }

    commitOffsets(groupId, offsets);
    ASSERT_EQ(12, m_groupStore.getGroupOffsetTable()->activeTupleCount());
}

/*
 * Test that fetching offsets retunrs the appropriate responses
 */
TEST_F(GroupStoreTest, FetchOffsets) {
    NValue groupId = ValueFactory::getTempStringValue("groupId");
    CommitOffsets offsets;

    {
        Group group(m_groupStore, groupId, 1000, 5, ValueFactory::getNullStringValue(), ValueFactory::getTempStringValue("protocol"));
        upsertGroup(group);
    }

    std::vector<NValue> topics = { ValueFactory::getTempStringValue("topic1"), ValueFactory::getTempStringValue(
            "topic2"), ValueFactory::getTempStringValue("topic3") };
    {
        std::vector<OffsetCommitRequestPartition> partitions = {
                OffsetCommitRequestPartition(1, 200, 0, ""),
                OffsetCommitRequestPartition(2, 500, 5, "mine"),
                OffsetCommitRequestPartition(3, 600, -1, "something"),
                OffsetCommitRequestPartition(4, 40, 2, "other")};
        for (auto topic : topics) {
            offsets.emplace(topic, partitions);
        }
    }
    commitOffsets(groupId, offsets);

    // Fetch all partitions from one topic
    {
        FetchOffsets fetch;
        fetch[topics[0]] = { 1, 2, 3, 4 };
        fetch[topics[1]] = { 1, 2, 3, 4 };

        OffsetFetchResponse response = fetchOffsets(groupId, fetch);
        auto responseTopics = response.topics();
        ASSERT_EQ(2, responseTopics.size());
        for (auto topic : responseTopics) {
            ASSERT_TRUE(topic.topic() == topics[0] || topic.topic() == topics[1]);
            ASSERT_EQ(4, topic.partitions().size());
            for (auto partition : topic.partitions()) {
                auto expected = offsets[topic.topic()][partition.partitionIndex() - 1];
                ASSERT_EQ(expected.partitionIndex(), partition.partitionIndex());
                ASSERT_EQ(expected.offset(), partition.offset());
                ASSERT_EQ(expected.leaderEpoch(), partition.leaderEpoch());
                ASSERT_EQ(expected.metadata(), partition.metadata());
            }
        }
    }

    // Fetching partitions/topics which do not exist return unknown offset value
    {
        FetchOffsets fetch;
        fetch[topics[0]] = { 0, 5, 6 };
        NValue unknownTopic = ValueFactory::getTempStringValue("unknown");
        NValue unknownMetadata = ValueFactory::getTempStringValue("");
        fetch[unknownTopic] = {1, 2, 3};

        OffsetFetchResponse response = fetchOffsets(groupId, fetch);
        auto responseTopics = response.topics();
        ASSERT_EQ(2, responseTopics.size());
        for (auto topic : responseTopics) {
            ASSERT_TRUE(topic.topic() == topics[0] || topic.topic() == unknownTopic);
            ASSERT_EQ(3, topic.partitions().size());
            for (auto partition : topic.partitions()) {
                ASSERT_EQ(-1, partition.offset());
                ASSERT_EQ(-1, partition.leaderEpoch());
                ASSERT_EQ(unknownMetadata, partition.metadata());
            }
        }
    }

    // Fetch of no topics returns all
    {
        FetchOffsets fetch;
        OffsetFetchResponse response = fetchOffsets(groupId, fetch);
        auto responseTopics = response.topics();
        ASSERT_EQ(3, responseTopics.size());
        for (auto topic : responseTopics) {
            ASSERT_NE(topics.end(), std::find(topics.begin(), topics.end(), topic.topic()));
            ASSERT_EQ(4, topic.partitions().size());
            for (auto partition : topic.partitions()) {
                auto expected = offsets[topic.topic()][partition.partitionIndex() - 1];
                ASSERT_EQ(expected.partitionIndex(), partition.partitionIndex());
                ASSERT_EQ(expected.offset(), partition.offset());
                ASSERT_EQ(expected.leaderEpoch(), partition.leaderEpoch());
                ASSERT_EQ(expected.metadata(), partition.metadata());
            }
        }
    }

}

/*
 * Test that deleting a group deletes the group all members and all offsets
 */
TEST_F(GroupStoreTest, DeleteGroup) {
    NValue groupId = ValueFactory::getTempStringValue("groupId");
    NValue groupId2 = ValueFactory::getTempStringValue("groupId2");
    NValue leader = ValueFactory::getTempStringValue("leader");
    NValue protocol = ValueFactory::getTempStringValue("protocol");

    // Insert group and offsets
    RandomData scratch(128);
    std::vector<NValue> topics = { ValueFactory::getTempStringValue("topic1"), ValueFactory::getTempStringValue(
            "topic2"), ValueFactory::getTempStringValue("topic3") };
    std::vector<OffsetCommitRequestPartition> partitions = {
            OffsetCommitRequestPartition(1, 200, 0, ""),
            OffsetCommitRequestPartition(2, 500, 5, "mine"),
            OffsetCommitRequestPartition(3, 600, -1, "something"),
            OffsetCommitRequestPartition(4, 40, 2, "other")};

    {
        Group group(m_groupStore, groupId, 1000, 5, leader, protocol);
        group.getOrCreateMember(generateGroupMemberid()).update(ValueFactory::getTempStringValue("truc"),
                ValueFactory::getTempStringValue("bidule"), 1000, 2000, ValueFactory::getNullStringValue(),
                ValueFactory::getTempBinaryValue(&scratch, 64), ValueFactory::getTempBinaryValue(&scratch[64], 64));
        group.getOrCreateMember(generateGroupMemberid()).update(ValueFactory::getTempStringValue("truc"),
                ValueFactory::getTempStringValue("bidule"), 1000, 2000, ValueFactory::getNullStringValue(),
                ValueFactory::getTempBinaryValue(&scratch, 64), ValueFactory::getTempBinaryValue(&scratch[64], 64));

        upsertGroup(group);

        CommitOffsets offsets;


        for (auto topic : topics) {
            offsets.emplace(topic, partitions);
        }
        commitOffsets(groupId, offsets);
    }

    // Create a second group and offsets
    {
        Group group(m_groupStore, groupId2, 1000, 5, leader, protocol);
        group.getOrCreateMember(generateGroupMemberid()).update(ValueFactory::getTempStringValue("truc"),
                ValueFactory::getTempStringValue("bidule"), 1000, 2000, ValueFactory::getNullStringValue(),
                ValueFactory::getTempBinaryValue(&scratch, 64), ValueFactory::getTempBinaryValue(&scratch[64], 64));
        group.getOrCreateMember(generateGroupMemberid()).update(ValueFactory::getTempStringValue("truc"),
                ValueFactory::getTempStringValue("bidule"), 1000, 2000, ValueFactory::getNullStringValue(),
                ValueFactory::getTempBinaryValue(&scratch, 64), ValueFactory::getTempBinaryValue(&scratch[64], 64));

        upsertGroup(group);

        CommitOffsets offsets;


        for (auto topic : topics) {
            offsets.emplace(topic, partitions);
        }
        commitOffsets(groupId2, offsets);
    }

    ASSERT_EQ(2, m_groupStore.getGroupTable()->activeTupleCount());
    ASSERT_EQ(4, m_groupStore.getGroupMemberTable()->activeTupleCount());
    ASSERT_EQ(24, m_groupStore.getGroupOffsetTable()->activeTupleCount());

    m_groupStore.deleteGroup(groupId);

    ASSERT_EQ(1, m_groupStore.getGroupTable()->activeTupleCount());
    ASSERT_EQ(2, m_groupStore.getGroupMemberTable()->activeTupleCount());
    ASSERT_EQ(12, m_groupStore.getGroupOffsetTable()->activeTupleCount());

    ASSERT_FALSE(Group(m_groupStore, groupId).isInTable());
    FetchOffsets fetch;
    ASSERT_EQ(0, fetchOffsets(groupId, fetch).topics().size());

    Group group2(m_groupStore, groupId2);
    ASSERT_TRUE(group2.isInTable());
    ASSERT_EQ(2, group2.getMembers().size());
    OffsetFetchResponse response = fetchOffsets(groupId2, fetch);
    ASSERT_EQ(3, response.topics().size());
    for (auto topic : response.topics()) {
        ASSERT_EQ(4, topic.partitions().size());
    }
}

/*
 * Test that fetch groups returns all groups and only as much as requested by the caller
 */
TEST_F(GroupStoreTest, FetchGroups) {
    std::unordered_map<NValue, std::unique_ptr<Group>> groups;

    int maxSize = 0;
    int totalSize = 0;
    for (int i = 0; i < 20; ++i) {
        RandomData scratch(128);

        int len = ::snprintf(&scratch, scratch.len(), "groupId_%d", i);
        NValue groupId = ValueFactory::getTempStringValue(&scratch, len);
        len = ::snprintf(&scratch, scratch.len(), "leaderId_%d", i);
        NValue leader = ValueFactory::getTempStringValue(&scratch, len);;
        len = ::snprintf(&scratch, scratch.len(), "protocol_%d", i);
        NValue protocol = ValueFactory::getTempStringValue(&scratch, len);
        Group* group = new Group(m_groupStore, groupId, 1000, 5, leader, protocol);
        groups[group->getGroupId()].reset(group);

        for (int j = 0 ; j < 10; ++j) {
            int len = ::snprintf(&scratch, scratch.len(), "memberId_%d", i);
            NValue memberId = ValueFactory::getTempStringValue(&scratch, len);
            group->getOrCreateMember(memberId).update(ValueFactory::getTempStringValue("truc"),
                    ValueFactory::getTempStringValue("bidule"), 200, 500, memberId,
                    ValueFactory::getTempBinaryValue(&scratch, scratch.len() - j),
                    ValueFactory::getTempBinaryValue(&scratch, scratch.len() - (j * 2)));
        }

        int serializedSize = group->serializedSize();
        totalSize += serializedSize;
        if (serializedSize > maxSize) {
            maxSize = serializedSize;
        }
        upsertGroup(*group);
    }

    char buffer[totalSize + 128];

    {
        ReferenceSerializeOutput out(buffer, sizeof(buffer));
        int res = m_groupStore.fetchGroups(sizeof(buffer), ValueFactory::getNullStringValue(), out);
        ASSERT_EQ(0, res);
    }

    // Big enough fetch should get all groups
    {
        ReferenceSerializeInputBE in(buffer, sizeof(buffer));
        ASSERT_EQ(totalSize + sizeof(int32_t), in.readInt());
        int count = in.readInt();
        ASSERT_EQ(groups.size(), count);
    }

    // Small fetch should only have one group and iterate through them
    {
        int loops = 0;
        NValue groupId = ValueFactory::getNullStringValue();
        int res;
        do {
            ++loops;
            ReferenceSerializeOutput out(buffer, sizeof(buffer));
            res = m_groupStore.fetchGroups(maxSize * 2 - 10, groupId, out);
            ASSERT_EQ(loops == groups.size() ? 0 : 1, res);
            ReferenceSerializeInputBE in(buffer, sizeof(buffer));
            int lenGroup = in.readInt();
            ASSERT_EQ(1, in.readInt());

            int lengroupId = in.readInt();
            char groupIdBytes[lengroupId];
            in.readBytes(groupIdBytes, lengroupId);
            groupId = ValueFactory::getTempStringValue(groupIdBytes, lengroupId);

            auto group = groups.find(groupId);
            ASSERT_NE(group, groups.end());
            ASSERT_EQ(group->second->serializedSize() + sizeof(int32_t), lenGroup);
        } while(res == 1);
    }
}

/*
 * Test that when bad messages are received exceptions are thrown
 */
TEST_F(GroupStoreTest, BadMessages) {
    char scratch[256];
    ReferenceSerializeOutput message(scratch, sizeof(scratch));
    message.writeInt(3);
    message.writeInt(50);
    message.writeTextString("abc");

    char outScratch[256];
    ReferenceSerializeOutput out(outScratch, sizeof(outScratch));

    NValue groupId = ValueFactory::getTempStringValue("groupId");
    {
        Group group(m_groupStore, groupId, 1000, 5, ValueFactory::getNullStringValue(), ValueFactory::getTempStringValue("protocol"));
        upsertGroup(group);
    }

    // try commit
    try {
        ReferenceSerializeInputBE in(scratch, message.position());
        m_groupStore.commitOffsets(0, 7, groupId, in, out);
        FAIL("should have thrown SerializableEEException");
    } catch (const SerializableEEException &e) {
        ASSERT_EQ(VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_INVALID_MESSAGE, e.getType());
    }
    ASSERT_EQ(0, out.position());

    // try fetch
    try {
        ReferenceSerializeInputBE in(scratch, message.position());
        m_groupStore.fetchOffsets(7, groupId, in, out);
        FAIL("should have thrown SerializableEEException");
    } catch (const SerializableEEException &e) {
        ASSERT_EQ(VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_INVALID_MESSAGE, e.getType());
    }
    ASSERT_EQ(0, out.position());
}

/*
 * Test that deleting expired offsets only deletes offsets for standalone groups that have expired
 */
TEST_F(GroupStoreTest, DeleteExpiredOffsets) {
    NValue regularGroupId = ValueFactory::getTempStringValue("regular");
    NValue standaloneGroupId = ValueFactory::getTempStringValue("standalone");

    Group regular(m_groupStore, regularGroupId, 1, 2, ValueFactory::getTempStringValue("leader"),
            ValueFactory::getTempStringValue("protocol"));
    upsertGroup(regular);

    // For standalone group use an empty protocol (not null)
    Group standalone(m_groupStore, standaloneGroupId, 1, 2, ValueFactory::getNullStringValue(),
            ValueFactory::getTempStringValue(""));
    upsertGroup(standalone);

    std::vector<NValue> topics = { ValueFactory::getTempStringValue("topic1"), ValueFactory::getTempStringValue(
            "topic2"), ValueFactory::getTempStringValue("topic3") };
    std::unordered_set<int> allPartitions = { 1, 2, 3, 4 };

    for (int partition : allPartitions) {
        CommitOffsets offsets;
        std::vector<OffsetCommitRequestPartition> partitions = { OffsetCommitRequestPartition(partition,
                partition * 100, 0, "") };
        for (auto topic : topics) {
            offsets.emplace(topic, partitions);
        }
        commitOffsets(regularGroupId, offsets, partition * 100);
        commitOffsets(standaloneGroupId, offsets, partition * 100);
    }

    std::function<void(const NValue&, int, std::unordered_set<int>)> validateOffsets = [this](const NValue& groupId,
            int topicCount, std::unordered_set<int> expectedPartitions) {
        int offsetCount = 0;
        GroupOffset::visitAll(m_groupStore, groupId,
                [this, &expectedPartitions, &offsetCount](const GroupOffset& offset) {
                    ASSERT_EQ(1, expectedPartitions.count(offset.getPartition()));
                    ++offsetCount;
                });
        ASSERT_EQ(topicCount * expectedPartitions.size(), offsetCount);
    };

    validateOffsets(regularGroupId, topics.size(), allPartitions);
    validateOffsets(standaloneGroupId, topics.size(), allPartitions);

    // Shouldn't delete any
    m_groupStore.deleteExpiredOffsets(0);
    validateOffsets(regularGroupId, topics.size(), allPartitions);
    validateOffsets(standaloneGroupId, topics.size(), allPartitions);

    // Should only delete the first 2 partitions from standalone group
    m_groupStore.deleteExpiredOffsets(201);
    validateOffsets(regularGroupId, topics.size(), allPartitions);
    validateOffsets(standaloneGroupId, topics.size(), { 3, 4 });

    // Should delete all offsets from standalone group
    m_groupStore.deleteExpiredOffsets(1000);
    validateOffsets(regularGroupId, topics.size(), allPartitions);
    validateOffsets(standaloneGroupId, topics.size(), { });
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
