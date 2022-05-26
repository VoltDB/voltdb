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

#include "storage/ExportTupleStream.h"
#include "topics/encode/Encoder.h"
#include "storage/streamedtable.h"

namespace voltdb {

class TopicTupleStream: public voltdb::ExportTupleStream {
public:
    /**
     * Create a new TopicTupleStream instance.
     *
     * @param stream wich backs the topic
     * @param topic catalog object for the topic
     * @param partitionId of this partition
     * @param siteId of this site
     * @param generation current generation ID
     */
    static TopicTupleStream* create(const StreamedTable& stream, const catalog::Topic& topic, CatalogId partitionId,
            int64_t siteId, int64_t generation);

    /**
     * Find the catalog topic object associated with stream if a TopicTupleStream is needed for stream
     *
     * @param stream to search for associated topic
     * @param database in which to look for the topic
     * @return Topic instance if the stream requires a TopicTupleStream or nullptr
     */
    static const catalog::Topic* getTopicForStream(const StreamedTable& stream, const catalog::Database& database);

    virtual ~TopicTupleStream() {}

    size_t appendTuple(VoltDBEngine* engine, int64_t txnId, int64_t seqNo, int64_t uniqueId, const TableTuple& tuple,
            int partitionColumn, ExportTupleStream::STREAM_ROW_TYPE type) override;

    void update(const StreamedTable& table, const catalog::Database& database) override;

    // property keys for topics. These are a duplicate of what is in TopicProperties.java
    static const std::string PROP_STORE_ENCODED;
    static const std::string PROP_TOPIC_FORMAT;
    static const std::string PROP_TOPIC_FORMAT_VALUE;
    static const std::string PROP_CONSUMER_FORMAT;
    static const std::string PROP_CONSUMER_FORMAT_KEY;
    static const std::string PROP_CONSUMER_FORMAT_VALUE;
    static const std::string PROP_CONSUMER_KEY;
    static const std::string PROP_CONSUMER_VALUE;

protected:
    ExportStreamBlock* allocateBlock(char* buffer, size_t length, int64_t uso) const override {
        return new TopicStreamBlock(buffer, m_headerSpace, length, uso);
    }

private:
    TopicTupleStream(CatalogId partitionId, int64_t siteId, int64_t generation, const std::string &tableName,
            topics::TupleEncoder* keyEncoder, topics::TupleEncoder* valueEncoder) :
        ExportTupleStream(partitionId, siteId, generation, tableName),
        m_keyEncoder(keyEncoder), m_valueEncoder(valueEncoder) {}

    /**
     * Create the key and value encoders that are defined by the properties in topic
     *
     * @param stream which backs the topic
     * @topic instance describing the topic configuration
     * @return a pair of key encoder and value encoder
     */
    static std::pair<topics::TupleEncoder*, topics::TupleEncoder*> createEncoders(const StreamedTable& table,
            const catalog::Topic& topic);

    /**
     * Create either a key or value encoder for a topic
     *
     * @param stream which backs the topic
     * @param formatKeys ordered list of properties to search for user specifed format
     * @param columnsKey property key to look up user defined columns for encoding
     * @param defaultColumns default value to use if there is no user defined columns
     * @param schemaId to associate with the encoder
     * @param props defined by the user
     * @return created encoders
     */
    static topics::TupleEncoder* createEncoder(const StreamedTable& stream, const std::vector<std::string> formatKeys,
            const std::string& columnsKey, const std::string& defaultColumns, int32_t schemaId,
            const topics::TopicProperties& props);

    std::unique_ptr<topics::TupleEncoder> m_keyEncoder;
    std::unique_ptr<topics::TupleEncoder> m_valueEncoder;
};

}
