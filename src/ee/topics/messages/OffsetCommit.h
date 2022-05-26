/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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
#include "topics/messages/CheckedSerializeInput.h"
#include "topics/messages/Message.h"

#define TOPICS_ERROR_NONE 0

namespace voltdb { namespace topics {

/**
 * Request class for committing an offset for a partition
 */
class OffsetCommitRequestPartition {

public:
    OffsetCommitRequestPartition(const int16_t version, CheckedSerializeInput& request);

    // Construct a request by hand. Only really used by tests
    OffsetCommitRequestPartition(int32_t partitionIndex, int64_t offset, int32_t leaderEpoch,
            const std::string& metadata) :
            m_partitionIndex(partitionIndex), m_offset(offset), m_leaderEpoch(leaderEpoch), m_timestamp(-1),
            m_metadata(ValueFactory::getTempStringValue(metadata)) {}

    inline const int32_t partitionIndex() const {
        return m_partitionIndex;
    }

    inline const int64_t offset() const {
        return m_offset;
    }

    inline const int32_t leaderEpoch() const {
        return m_leaderEpoch;
    }

    inline const int64_t timestamp() const {
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

// Response classes

/**
 * Response to committing an offset for a topic partition
 */
class OffsetCommitResponsePartition: protected ResponseComponent {

public:
    OffsetCommitResponsePartition(int32_t partitionIndex) :
            m_partitionIndex(partitionIndex) {}

    void write(const int16_t version, SerializeOutput &out) const override;

    inline const int32_t partitionIndex() const {
        return m_partitionIndex;
    }

private:
    const int32_t m_partitionIndex;
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
class OffsetCommitResponse: public Response {

public:
    OffsetCommitResponse() = default;

    inline const std::vector<OffsetCommitResponseTopic>& topics() const {
        return m_topics;
    }

    template <typename... Args>
    inline OffsetCommitResponseTopic& addTopic(Args&&... args) {
        m_topics.emplace_back(std::forward<Args>(args)...);
        return m_topics.back();
    }

    void write(const int16_t version, SerializeOutput& out) const override;

private:
    // Per topic responses
    std::vector<OffsetCommitResponseTopic> m_topics;
};

} }
