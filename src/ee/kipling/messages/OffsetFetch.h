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
 * Request for which partitions within a topic are being requested
 */
class OffsetFetchRequestTopic: protected RequestComponent {

public:
    OffsetFetchRequestTopic(const int16_t version, SerializeInputBE &request);

    inline const NValue& topic() const {
        return m_topic;
    }

    inline const std::vector<int32_t>& partitions() const {
        return m_partitions;
    }

private:
    const NValue m_topic;
    std::vector<int32_t> m_partitions;
};

/**
 * Request for fetching specific topic partition offsets for a group
 */
class OffsetFetchRequest: public GroupRequest {

public:
    OffsetFetchRequest(const int16_t version, const NValue& groupId, SerializeInputBE &request);

    inline const std::vector<OffsetFetchRequestTopic>& topics() const {
        return m_topics;
    }

private:
    std::vector<OffsetFetchRequestTopic> m_topics;
};

// Response classes

/**
 * Response to a specific topic partition offset fetch request
 */
class OffsetFetchResponsePartition: protected ResponseComponent {

public:
    OffsetFetchResponsePartition(int32_t partitionIndex, int64_t offset, int32_t leaderEpoch,
            const NValue& metadata) :
            m_partitionIndex(partitionIndex), m_offset(offset), m_leaderEpoch(leaderEpoch), m_metadata(metadata) {}

    OffsetFetchResponsePartition(int32_t partitionIndex, Error error) :
            m_partitionIndex(partitionIndex), m_error(error) {}

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

    inline const Error error() const {
        return m_error;
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
    // Error code for this individual partition
    Error m_error = Error::NONE;
};

class OffsetFetchResponseTopic: protected ResponseComponent {

public:
    OffsetFetchResponseTopic(const NValue& topic) : m_topic(topic) {}

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
class OffsetFetchResponse: public Response<OffsetFetchResponse> {

public:
    void write(const int16_t version, SerializeOutput &out) const override;

    inline const std::vector<OffsetFetchResponseTopic>& topics() const {
        return m_topics;
    }

    template <typename... Args>
    inline OffsetFetchResponseTopic& addTopic(Args&&... args) {
        m_topics.emplace_back(std::forward<Args>(args)...);
        return m_topics.back();
    }

private:
    // Per topic responses
    std::vector<OffsetFetchResponseTopic> m_topics;
};

} }
