/* This file is part of VoltDB.
 * Copyright (C) 2019 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <stdint.h>
#include <string>

#include "common/serializeio.h"
#include "kipling/messages/Error.h"
#include "kipling/messages/Message.h"

namespace voltdb { namespace kipling {

/**
 * Request class for committing an offset for a partition
 */
class OffsetCommitRequestPartition: protected RequestComponent {

public:
    OffsetCommitRequestPartition(const int16_t version, SerializeInputBE &request);

    // Construct a request by hand. Only really used by tests
    OffsetCommitRequestPartition(const int16_t version, int32_t partitionIndex, int64_t offset, int32_t leaderEpoch,
            const NValue &metadata) :
            m_partitionIndex(partitionIndex), m_offset(offset), m_leaderEpoch(leaderEpoch), m_timestamp(-1),
            m_metadata(metadata) {}

    inline const int32_t partitionIndex() const {
        return m_partitionIndex;
    }

    inline const int64_t offset() const {
        return m_offset;
    }

    inline const int32_t leaderEpoch() const {
        return m_leaderEpoch;
    }

    inline const int32_t timestamp() const {
        return m_timestamp;
    }

    inline const NValue& metadata() const {
        return m_metadata;
    }

private:
    // Partition index/id
    const int32_t m_partitionIndex;
    // Offset to commit
    const int64_t m_offset;
    // Partition leader epoch at time of commit
    int32_t m_leaderEpoch = -1;
    // Legacy timestamp just here for backwards compatibility and readability
    int64_t m_timestamp = -1L;
    // Opaque metadata associated with this committed offset
    NValue m_metadata;
};

/**
 * Request class for committing offsets for partitions in a single topic
 */
class OffsetCommitRequestTopic: protected RequestComponent {

public:
    OffsetCommitRequestTopic(const int16_t version, SerializeInputBE &request);

    inline const NValue& topic() const {
        return m_topic;
    }

    inline const std::vector<OffsetCommitRequestPartition>& partitions() const {
        return m_partitions;
    }

private:
    // Name of topic
    const NValue m_topic;
    // List of partition offsets to commit
    std::vector<OffsetCommitRequestPartition> m_partitions;
};

/**
 * Request class for committing offsets for a set of topic and partitions
 */

class OffsetCommitRequest: public GroupRequest {

public:
    OffsetCommitRequest(const int16_t version, const NValue& groupId, SerializeInputBE &request);

    inline const int32_t generationId() const {
        return m_generationId;
    }

    inline const NValue& memberId() const {
        return m_memberId;
    }

    inline const NValue& groupInstanceId() const {
        return m_groupInstanceId;
    }

    inline const std::vector<OffsetCommitRequestTopic>& topics() {
        return m_topics;
    }

private:
    // Generation ID of the group
    int32_t m_generationId = -1;
    // ID of the member committing the offsets
    NValue m_memberId;
    // Group instance ID of the member committing the offsets of exists
    NValue m_groupInstanceId;
    // Topics which have offsets to commit
    std::vector<OffsetCommitRequestTopic> m_topics;
};

// Response classes

/**
 * Response to committing an offset for a topic partition
 */
class OffsetCommitResponsePartition: protected ResponseComponent {

public:
    OffsetCommitResponsePartition(int32_t partitionIndex, Error error) :
            m_partitionIndex(partitionIndex), m_error(error) {}

    OffsetCommitResponsePartition(int32_t partitionIndex) :
                m_partitionIndex(partitionIndex), m_error(Error::NONE) {}

    void write(const int16_t version, SerializeOutput &out) const override;

    inline const int32_t partitionIndex() const {
        return m_partitionIndex;
    }

    inline const Error error() const {
        return m_error;
    }

private:
    const int32_t m_partitionIndex;
    const Error m_error;
};

/*
 * Responses for committing offsets for a topic
 */
class OffsetCommitResponseTopic: protected ResponseComponent {

public:
    OffsetCommitResponseTopic(const NValue& topic) : m_topic(topic) {}

    void write(const int16_t version, SerializeOutput &out) const override;

    inline const NValue& topic() const {
        return m_topic;
    }

    inline const std::vector<OffsetCommitResponsePartition>& partitions() const {
        return m_partitions;
    }

    template <typename... Args>
    inline OffsetCommitResponsePartition& addPartition(Args&&... args) {
        m_partitions.emplace_back(std::forward<Args>(args)...);
        return m_partitions.back();
    }

private:
    // Name of topic
    const NValue m_topic;
    // Responses to individual partitions which were committed
    std::vector<OffsetCommitResponsePartition> m_partitions;
};

/*
 * Response to OffsetCommitRequest this does not extend Response because it does not have an Error code
 */
class OffsetCommitResponse: protected ResponseComponent {

public:
    void write(const int16_t version, SerializeOutput &out) const override;

    inline const int32_t throttleTimeMs() const {
        return m_throttleTimeMs;
    }

    inline OffsetCommitResponse& throttleTimeMs(int32_t thottleTimeMs) {
        m_throttleTimeMs = thottleTimeMs;
        return *this;
    }

    inline const std::vector<OffsetCommitResponseTopic>& topics() const {
        return m_topics;
    }

    template <typename... Args>
    inline OffsetCommitResponseTopic& addTopic(Args&&... args) {
        m_topics.emplace_back(std::forward<Args>(args)...);
        return m_topics.back();
    }

private:
    int32_t m_throttleTimeMs = 0;
    // Per topic responses
    std::vector<OffsetCommitResponseTopic> m_topics;
};

} }
