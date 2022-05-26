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

#include "storage/TopicTupleStream.h"
#include "common/serializeio.h"

#include "catalog/database.h"
#include "catalog/table.h"
#include "catalog/topic.h"
#include "catalog/property.h"

#include "topics/encode/AvroEncoder.h"
#include "topics/encode/CsvEncoder.h"

#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/trim.hpp>

namespace voltdb {

// property keys for topics. These are a duplicate of what is in TopicProperties.java
const std::string TopicTupleStream::PROP_STORE_ENCODED = "topic.store.encoded";
const std::string TopicTupleStream::PROP_TOPIC_FORMAT = "topic.format";
const std::string TopicTupleStream::PROP_TOPIC_FORMAT_VALUE = "topic.format.value";
const std::string TopicTupleStream::PROP_CONSUMER_FORMAT = "consumer.format";
const std::string TopicTupleStream::PROP_CONSUMER_FORMAT_KEY = "consumer.format.key";
const std::string TopicTupleStream::PROP_CONSUMER_FORMAT_VALUE = "consumer.format.value";
const std::string TopicTupleStream::PROP_CONSUMER_KEY = "consumer.key";
const std::string TopicTupleStream::PROP_CONSUMER_VALUE = "consumer.value";
static const std::string s_undefinedFormat = "UNDEFINED";


TopicTupleStream* TopicTupleStream::create(const StreamedTable& stream, const catalog::Topic& topic,
        CatalogId partitionId, int64_t siteId, int64_t generation) {
    auto encoders = createEncoders(stream, topic);
    return new TopicTupleStream(partitionId, siteId, generation, stream.name(), encoders.first, encoders.second);
}

const catalog::Topic* TopicTupleStream::getTopicForStream(const StreamedTable& stream,
        const catalog::Database& database) {
    const catalog::Topic *topic = nullptr;
    const std::string& topicName = database.tables().get(stream.name())->topicName();
    if (topicName.length() > 0) {
        topic = database.topics().get(topicName);
        if (topic != nullptr) {
            const catalog::Property *encoded = topic->properties().get(PROP_STORE_ENCODED);
            // TODO invert this so nullptr keeps topic when the default for PROP_STORE_ENCODED is set to true
            if (encoded == nullptr || !MiscUtil::parseBool(&encoded->value())) {
                topic = nullptr;
            }
        }
    }
    return topic;
}

std::pair<topics::TupleEncoder*, topics::TupleEncoder*> TopicTupleStream::createEncoders(const StreamedTable& stream,
            const catalog::Topic& topic) {
    vassert(boost::iequals(stream.name(), topic.streamName()));

    topics::TopicProperties props;
    for (auto& prop : topic.properties()) {
        props[prop.second->name()] = prop.second->value();
    }

    std::vector<std::string> keyFormats { PROP_CONSUMER_FORMAT_KEY, PROP_CONSUMER_FORMAT, PROP_TOPIC_FORMAT };
    topics::TupleEncoder *keyEncoder = createEncoder(stream, keyFormats, PROP_CONSUMER_KEY, "",
            topic.consumerKeySchemaId(), props);

    std::vector<std::string> valueFormats { PROP_CONSUMER_FORMAT_VALUE, PROP_CONSUMER_FORMAT, PROP_TOPIC_FORMAT_VALUE,
            PROP_TOPIC_FORMAT };
    topics::TupleEncoder *valueEncoder = createEncoder(stream, valueFormats, PROP_CONSUMER_VALUE, "*",
            topic.consumerValueSchemaId(), props);

    return {keyEncoder, valueEncoder};
}

topics::TupleEncoder* TopicTupleStream::createEncoder(const StreamedTable& stream,
        const std::vector<std::string> formatKeys, const std::string& columnsKey, const std::string& defaultColumns,
        int32_t schemaId, const topics::TopicProperties& props) {
    // Determine which columns are to be encoded
    auto columnsEntry = props.find(columnsKey);
    std::string columnsCsv = columnsEntry == props.end() ? defaultColumns : columnsEntry->second;

    // No columns are to be encoded so return null encoder
    if (columnsCsv.length() == 0) {
        return new topics::NullEncoder();
    }

    std::vector<int32_t> columnIndexes;
    if (columnsCsv == "*") {
        // All columns are to be encoded
        columnIndexes.resize(stream.columnCount());
        std::iota(columnIndexes.begin(), columnIndexes.end(), 0);
    } else {
        // Find the index of the selected columns
        const std::vector<std::string>& columnNames = stream.getColumnNames();
        int start = 0;

        do {
            int commaIndex = columnsCsv.find(",", start);
            int end = commaIndex == std::string::npos ? columnsCsv.length() : commaIndex;
            std::string column = columnsCsv.substr(start, end - start);
            boost::trim(column);
            __attribute__((unused)) bool found = false;
            for (int i = 0; i < columnNames.size(); ++i) {
                if (boost::iequals(column, columnNames[i])) {
                    columnIndexes.push_back(i);
                    found = true;
                    break;
                }
            }
            vassert(found);
            start = end + 1;
        } while (start < columnsCsv.length());
    }

    // Find the user selected encoding
    const std::string* encoding = &s_undefinedFormat;
    for (const std::string& key : formatKeys) {
        auto entry = props.find(key);
        if (entry != props.end() && s_undefinedFormat != entry->second) {
            encoding = &entry->second;
        }
    }

    const TupleSchema* schema = stream.schema();
    if (s_undefinedFormat == *encoding) {
        if (columnIndexes.size() == 1) {
            int32_t index = columnIndexes[0];
            const TupleSchema::ColumnInfo* info = schema->getColumnInfo(index);
            switch (static_cast<ValueType>(info->type)) {
            case ValueType::tINTEGER:
                return new topics::SingleValueEncoder<topics::IntEncoder>(index);
            case ValueType::tBIGINT:
                return new topics::SingleValueEncoder<topics::BigIntEncoder>(index);
            case ValueType::tDOUBLE:
                return new topics::SingleValueEncoder<topics::DoubleEncoder>(index);
            case ValueType::tVARCHAR:
            case ValueType::tVARBINARY:
                return new topics::SingleValueEncoder<topics::PlainVarLenEncoder>(index);
            default:
                return new topics::SingleValueEncoder<topics::ToStringEncoder>(index);
            }
        }
        return new topics::CsvEncoder(columnIndexes, props);
    }

    if (boost::iequals("AVRO", *encoding)) {
        return new topics::AvroEncoder(schemaId, *schema, columnIndexes, props);
    }
    if (boost::iequals("CSV", *encoding)) {
        return new topics::CsvEncoder(columnIndexes, props);
    }

    throwSerializableEEException("Unknown encoding: %s", encoding->c_str());
}

size_t TopicTupleStream::appendTuple(VoltDBEngine* engine, int64_t txnId, int64_t seqNo, int64_t uniqueId,
        const TableTuple& tuple, int partitionColumn, ExportTupleStream::STREAM_ROW_TYPE type) {

    // Transaction IDs for transactions applied to this tuple stream
    // should always be moving forward in time.
    if (txnId < m_openTxnId) {
        throwFatalException("Active transactions moving backwards: openTxnId is %jd, while the append txnId is %jd",
                (intmax_t) m_openTxnId, (intmax_t) txnId);
    }
    m_openTxnId = txnId;
    m_openUniqueId = uniqueId;

    int64_t timestampDelta =
            m_currBlock == nullptr || m_currBlock->getRowCount() == 0 ?
                    0 : UniqueId::tsInMillis(uniqueId) - UniqueId::tsInMillis(m_currBlock->lastSpUniqueId());

    int32_t offsetDelta = m_currBlock == nullptr ? 0 : m_currBlock->getRowCount();
    int32_t keySize = m_keyEncoder->sizeOf(tuple);
    int32_t valueSize = m_valueEncoder->sizeOf(tuple);

    size_t recordSize = sizeof(int8_t) /*attributes */
            + SerializeOutput::sizeOfVarLong(offsetDelta)
            + SerializeOutput::sizeOfVarLong(timestampDelta) + SerializeOutput::sizeOfVarLong(keySize)
            + SerializeOutput::sizeOfVarLong(valueSize) + SerializeOutput::sizeOfVarLong(0) /* no headers */;

    // Add key and value sizes if they actually will take up space
    if (keySize > 0) {
        recordSize += keySize;
    }
    if (valueSize > 0) {
        recordSize += valueSize;
    }

    size_t totalSize = recordSize + SerializeOutput::sizeOfVarLong(recordSize);

    if (m_currBlock == nullptr || totalSize > m_currBlock->remaining()) {
        extendBufferChain(totalSize);
        if (offsetDelta != 0) {
            // New block so offset delta changed and that means sizes need to be recalculated
            recordSize -= SerializeOutput::sizeOfVarLong(offsetDelta) - SerializeOutput::sizeOfVarLong(0);
            offsetDelta = 0;
            totalSize = recordSize + SerializeOutput::sizeOfVarLong(recordSize);
        }
    }

    ReferenceSerializeOutput out(m_currBlock->mutableDataPtr(), m_currBlock->remaining());
    out.writeVarLong(recordSize);
    out.writeByte(0); // attributes. There are none
    out.writeVarLong(timestampDelta);
    out.writeVarLong(offsetDelta);
    out.writeVarLong(keySize);
    if (keySize >= 0) {
        __attribute__((unused)) int32_t written = m_keyEncoder->encode(out, tuple);
        vassert(keySize == written);
    }
    out.writeVarLong(valueSize);
    if (valueSize >= 0) {
        __attribute__((unused)) int32_t written = m_valueEncoder->encode(out, tuple);
        vassert(valueSize == written);
    }
    out.writeVarLong(0); // headers count

    return recordTupleAppended(totalSize, uniqueId);
}

void TopicTupleStream::update(const StreamedTable& stream, const catalog::Database& database) {
    const catalog::Topic* topic = getTopicForStream(stream, database);
    vassert(topic != nullptr);
    auto encoders = createEncoders(stream, *topic);
    m_keyEncoder.reset(encoders.first);
    m_valueEncoder.reset(encoders.second);
}

}
