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

namespace voltdb { namespace topics {

/**
 * Response to a specific topic partition offset fetch request
 */
class OffsetFetchResponsePartition: protected ResponseComponent {

public:
    OffsetFetchResponsePartition(int32_t partitionIndex, int64_t offset, int32_t leaderEpoch,
            const NValue& metadata) :
            m_partitionIndex(partitionIndex), m_offset(offset), m_leaderEpoch(leaderEpoch), m_metadata(metadata) {}

    OffsetFetchResponsePartition(int32_t partitionIndex) :
            m_partitionIndex(partitionIndex) {}

    OffsetFetchResponsePartition(int16_t version, CheckedSerializeInput& in);

    void write(const int16_t version, SerializeOutput &out) const override;

    inline const int32_t partitionIndex() const {
        return m_partitionIndex;
    }

    inline const int64_t offset() const {
        return m_offset;
    }

    inline const int32_t leaderEpoch() const {
        return m_leaderEpoch;
    }

    inline const NValue& metadata() const {
        return m_metadata;
    }

private:
    // Partition index/id
    const int32_t m_partitionIndex;
    // Last committed offset or -1 if no offset or error
    int64_t m_offset= -1;
    // Optional leader epoch which can be supplied with the offset
    int32_t m_leaderEpoch = 0;
    // Metadata associated with the offset
    NValue m_metadata;
};

/**
 * Response to all partitions requested in a topic
 */
class OffsetFetchResponseTopic: protected ResponseComponent {

public:
    OffsetFetchResponseTopic(const NValue& topic) : m_topic(topic) {}

    OffsetFetchResponseTopic(int16_t version, CheckedSerializeInput& in);

    void write(const int16_t version, SerializeOutput &out) const override;

    inline const NValue& topic() const {
        return m_topic;
    }

    inline const std::vector<OffsetFetchResponsePartition>& partitions() const {
        return m_partitions;
    }

    template <typename... Args>
    inline OffsetFetchResponsePartition& addPartition(Args&&... args) {
        m_partitions.emplace_back(std::forward<Args>(args)...);
        return m_partitions.back();
    }

private:
    // Name of topic
    const NValue m_topic;
    // Individual partition responses
    std::vector<OffsetFetchResponsePartition> m_partitions;
};

/*
 * Response to OffsetFetchRequest
 */
class OffsetFetchResponse: public Response {

public:
    OffsetFetchResponse() = default;

    OffsetFetchResponse(int16_t version, CheckedSerializeInput& in);

    inline const std::vector<OffsetFetchResponseTopic>& topics() const {
        return m_topics;
    }

    template <typename... Args>
    inline OffsetFetchResponseTopic& addTopic(Args&&... args) {
        m_topics.emplace_back(std::forward<Args>(args)...);
        return m_topics.back();
    }

    void write(const int16_t version, SerializeOutput& out) const override;

private:
    // Per topic responses
    std::vector<OffsetFetchResponseTopic> m_topics;
};

} }
