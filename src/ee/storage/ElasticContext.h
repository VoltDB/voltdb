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
#ifndef ELASTIC_CONTEXT_H_
#define ELASTIC_CONTEXT_H_

#include <vector>
#include <string>
#include "storage/ElasticScanner.h"
#include "storage/TableStreamer.h"
#include "storage/TableStreamerContext.h"
#include "storage/TupleBlock.h"

class DummyElasticTableStreamer;
class CopyOnWriteTest;

namespace voltdb {

class PersistentTable;
class TupleOutputStreamProcessor;

class ElasticContext : public TableStreamerContext
{

    friend bool TableStreamer::activateStream(PersistentTableSurgeon&, TupleSerializer&, TableStreamType, std::vector<std::string>&);
    friend class ::DummyElasticTableStreamer;
    friend class ::CopyOnWriteTest;

public:

    /**
     * Destructor.
     */
    virtual ~ElasticContext();

    /**
     * Activation/reactivation handler.
     */
    virtual ActivationReturnCode handleActivation(TableStreamType streamType, bool reactivate);

    /**
     * Deactivation handler.
     */
    virtual bool handleDeactivation(TableStreamType streamType);

    /**
     * Mandatory streamMore() handler.
     */
    virtual int64_t handleStreamMore(TupleOutputStreamProcessor &outputStreams,
                                     std::vector<int> &retPositions);

    /**
     * Optional tuple insert handler.
     */
    virtual bool notifyTupleInsert(TableTuple &tuple);

    /**
     * Optional tuple update handler.
     */
    virtual bool notifyTupleUpdate(TableTuple &tuple);

    /**
     * Optional tuple delete handler.
     */
    virtual bool notifyTupleDelete(TableTuple &tuple);

    /**
     * Optional tuple compaction handler.
     */
    virtual void notifyTupleMovement(TBPtr sourceBlock, TBPtr targetBlock,
                                     TableTuple &sourceTuple, TableTuple &targetTuple);

private:

    /**
     * Constructor - private so that only TableStreamer::activateStream() can call.
     */
    ElasticContext(PersistentTable &table,
                   PersistentTableSurgeon &surgeon,
                   int32_t partitionId,
                   TupleSerializer &serializer,
                   const std::vector<std::string> &predicateStrings,
                   size_t nTuplesPerCall = DEFAULT_TUPLES_PER_CALL);

    /**
     * Allow overriding how often index creation is throttled.
     */
    void setTuplesPerCall(size_t nTuplesPerCall) {
        m_nTuplesPerCall = nTuplesPerCall;
    }

    /**
     * Scanner for retrieving rows.
     */
    ElasticScanner m_scanner;

    /**
     * The maximum number of tuples to index per handleStreamMore() call.
     * It's non-const to allow tests to manipulate (e.g. CopyOnWriteTest).
     */
    size_t m_nTuplesPerCall;

    static const size_t DEFAULT_TUPLES_PER_CALL = 10000;
};

} // namespace voltdb

#endif // ELASTIC_CONTEXT_H_
