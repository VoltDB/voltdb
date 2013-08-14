/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
#ifndef ELASTIC_INDEX_READ_CONTEXT_H_
#define ELASTIC_INDEX_READ_CONTEXT_H_

#include <string>
#include <vector>
#include "common/types.h"
#include "storage/ElasticIndex.h"
#include "storage/TableStreamer.h"
#include "storage/TableStreamerContext.h"
#include "storage/TupleBlock.h"

namespace voltdb
{

class TableStreamer;
class PersistentTableSurgeon;
class TupleSerializer;
class TupleOutputStreamProcessor;

class ElasticIndexReadContext : public TableStreamerContext {

    friend bool TableStreamer::activateStream(PersistentTableSurgeon&, TupleSerializer&,
                                              TableStreamType, std::vector<std::string>&);

public:

    /**
     * Destructor.
     */
    virtual ~ElasticIndexReadContext();

    /**
     * Activation handler.
     */
    virtual bool handleActivation(TableStreamType streamType, bool reactivate);

    /**
     * Mandatory TableStreamContext override.
     */
    virtual int64_t handleStreamMore(TupleOutputStreamProcessor &outputStreams,
                                     std::vector<int> &retPositions);

private:

    /**
     * Construct a copy on write context for the specified table that will
     * serialize tuples using the provided serializer.
     * Private so that only TableStreamer::activateStream() can call.
     */
    ElasticIndexReadContext(PersistentTable &table,
                            PersistentTableSurgeon &surgeon,
                            int32_t partitionId,
                            TupleSerializer &serializer,
                            const std::vector<std::string> &predicateStrings);

    /**
     * Hash range for filtering.
     * The range specification is exclusive, specifically:
     *  from < to:
     *      from..to-1
     *  from >= to:
     *      from..max_int and min_int..to-1 (wraps around)
     * All possible value pairs are valid.
     */
    class HashRange
    {
    public:

        /**
         * Full constructor.
         */
        HashRange(int64_t from, int64_t to) : m_from(from), m_to(to)
        {}

        /**
         * Copy constructor.
         */
        HashRange(const HashRange &other) : m_from(other.m_from), m_to(other.m_to)
        {}

        /**
         * Return true if the range wraps around.
         */
        bool wrapsAround() const
        {
            return m_from >= m_to;
        }

        int64_t m_from;
        int64_t m_to;
    };

    /**
     * Parse and validate the hash range.
     */
    static HashRange parseHashRange(const std::vector<std::string> &predicateStrings);

    /// Active hash range.
    HashRange m_range;

    /// Elastic index iterator
    ElasticIndex::const_iterator m_iter;

    /// Set to true when iteration continues after wrapping around.
    bool m_wrappedAround;
};

}

#endif // ELASTIC_INDEX_READ_CONTEXT_H_
