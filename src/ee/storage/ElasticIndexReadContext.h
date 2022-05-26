/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
#include <boost/shared_ptr.hpp>
#include "common/types.h"
#include "storage/ElasticIndex.h"
#include "storage/TableStreamer.h"
#include "storage/TableStreamerContext.h"
#include "storage/TupleBlock.h"

namespace voltdb {
class TableStreamer;
class PersistentTableSurgeon;
class TupleOutputStreamProcessor;
class TupleSchema;
class TableTuple;

class ElasticIndexReadContext : public TableStreamerContext {
    friend bool TableStreamer::activateStream(PersistentTableSurgeon&, TableStreamType,
            const HiddenColumnFilter&, const std::vector<std::string>&);

public:
    /**
     * Destructor.
     */
    virtual ~ElasticIndexReadContext();

    /**
     * Activation handler.
     */
    virtual ActivationReturnCode handleActivation(TableStreamType streamType);

    /**
     * Deactivation handler.
     */
    virtual bool handleDeactivation(TableStreamType streamType);

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
                            const std::vector<std::string> &predicateStrings);

    /**
     * Parse and validate the hash range.
     * Checks that only one predicate string is provided.
     * Update rangeOut with parsed hash range.
     * Return true for success and false for failure.
     */
    static bool parseHashRange(const std::vector<std::string> &predicateStrings,
                               ElasticIndexHashRange &rangeOut);

    /**
     * Clean up after consuming indexed tuples.
     */
    void deleteStreamedTuples();

    /// Predicate strings (parsed in handleActivation()/handleReactivation()).
    const std::vector<std::string> &m_predicateStrings;

    /// Elastic index iterator
    boost::shared_ptr<ElasticIndexTupleRangeIterator> m_iter;

    /// Set to true after index was completely materialized.
    bool m_materialized;

    const HiddenColumnFilter m_filter;
};

}

#endif // ELASTIC_INDEX_READ_CONTEXT_H_
