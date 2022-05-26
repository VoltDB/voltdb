/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

#include <random>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "harness.h"
#include "topics/orm/Group.h"

using namespace voltdb;
using namespace voltdb::topics;

// Utility class to wrap a char[] of random data
class RandomData {
public:
    RandomData(size_t len) : m_len(len), m_data(new char[len]) {
        uint64_t *data = (uint64_t*) m_data;
        int len64 = len / sizeof(uint64_t);
        for (int i = 0; i < len64; ++i) {
            data[i] = s_mte();
        }

        int remainder = len % sizeof(uint64_t);
        if (remainder > 0) {
            uint64_t val = s_mte();
            for (int i = 0; i < remainder; ++i) {
                m_data[len - 1 - i] = (char) (val & 0xFF);
                val >>= sizeof(char);
            }
        }
    }

    ~RandomData() {
        delete[] m_data;
    }

    char& operator[](int offset) {
        vassert(offset < m_len);
        return m_data[offset];
    }

    char* operator&() {
        return m_data;
    }

    size_t len() {
        return m_len;
    }

private:
    static std::mt19937_64 s_mte;

    size_t m_len;
    char *m_data;
};

std::mt19937_64 RandomData::s_mte;

class GroupTestBase : public Test {
public:
    void validateGroupCommited(const GroupTables& tables, Group& expected) {
        Group actual(tables, expected.getGroupId());
        EXPECT_TRUE(actual.isInTable());

        ASSERT_EQ(expected.getGroupId(), actual.getGroupId());
        ASSERT_EQ(expected.getCommitTimestamp(), actual.getCommitTimestamp());
        ASSERT_EQ(expected.getGeneration(), actual.getGeneration());
        ASSERT_EQ(expected.getLeader(), actual.getLeader());
        ASSERT_EQ(expected.getProtocol(), actual.getProtocol());

        std::vector<GroupMember*> members = expected.getMembers();
        ASSERT_EQ(members.size(), actual.getMembers(true).size());
        for (auto member : members) {
            ASSERT_EQ(expected.getGroupId(), member->getGroupId());
            GroupMember* newMember = actual.getMember(member->getMemberId());
            ASSERT_TRUE(newMember);
            ASSERT_EQ(member->getMemberId(), newMember->getMemberId());
            ASSERT_EQ(member->getSessionTimeout(), newMember->getSessionTimeout());
            ASSERT_EQ(member->getRebalanceTimeout(), newMember->getRebalanceTimeout());
            ASSERT_EQ(member->getInstanceId(), newMember->getInstanceId());
            ASSERT_EQ(member->getProtocolMetadata(), newMember->getProtocolMetadata());
            ASSERT_EQ(member->getAssignments(), newMember->getAssignments());
        }
    }

    static NValue generateGroupMemberid() {
        boost::uuids::random_generator generator;
        boost::uuids::uuid memberUuid(generator());
        std::string memberId = boost::uuids::to_string(memberUuid);
        return ValueFactory::getTempStringValue(memberId);
    }
};
