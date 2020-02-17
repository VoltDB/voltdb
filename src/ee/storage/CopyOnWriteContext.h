/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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
#ifndef COPYONWRITECONTEXT_H_
#define COPYONWRITECONTEXT_H_

#include <string>
#include <vector>
#include <utility>
#include "common/TupleOutputStreamProcessor.h"
#include "storage/persistenttable.h"
#include "storage/TableStreamer.h"
#include "storage/TableStreamerContext.h"
#include "common/Pool.hpp"
#include "common/tabletuple.h"
#include <boost/scoped_ptr.hpp>
#include <boost/ptr_container/ptr_vector.hpp>

namespace voltdb {
class ParsedPredicate;
class TupleOutputStreamProcessor;
class PersistentTableSurgeon;

class CopyOnWriteContext : public TableStreamerContext {

    friend bool TableStreamer::activateStream(PersistentTableSurgeon&, TableStreamType,
            const  HiddenColumnFilter&, const std::vector<std::string>&);

public:

    virtual ~CopyOnWriteContext();

    /**
     * Activation handler.
     */
    virtual ActivationReturnCode handleActivation(TableStreamType streamType);

    /**
     * Reactivation handler.
     */
    virtual ActivationReturnCode handleReactivation(TableStreamType streamType);
    /**
     * Mandatory TableStreamContext override.
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

private:

    /**
     * Construct a copy on write context for the specified table that will
     * serialize tuples using the provided serializer.
     * Private so that only TableStreamer::activateStream() can call.
     */
    CopyOnWriteContext(PersistentTable &table,
                       PersistentTableSurgeon &surgeon,
                       int32_t partitionId,
                       const HiddenColumnFilter &hiddenColumnFilter,
                       const std::vector<std::string> &predicateStrings,
                       int64_t totalTuples);


    /**
     * Memory pool for string allocations
     */
    Pool m_pool;

    int64_t m_totalTuples;
    int64_t m_tuplesRemaining;
    int64_t m_serializationBatches;
    const bool m_replicated;
    const HiddenColumnFilter m_hiddenColumnFilter;
};

}

#endif /* COPYONWRITECONTEXT_H_ */
