/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
#ifndef RECOVERYCONTEXT_H_
#define RECOVERYCONTEXT_H_

#include "storage/tableiterator.h"
#include "storage/TableStreamer.h"
#include "storage/TableStreamerContext.h"
#include "common/DefaultTupleSerializer.h"

/*
 * A log of changes to tuple data that has already been sent to a recovering
 * partition as well as a mechanism to send messages containing recovery data.
 */
namespace voltdb {
class PersistentTable;
class PersistentTableSurgeon;
class ReferenceSerializeOutput;

class RecoveryContext : public TableStreamerContext {

    friend bool TableStreamer::activateStream(PersistentTableSurgeon&, TupleSerializer&,
                                              TableStreamType, const std::vector<std::string>&);

public:

    /*
     * Generate the next recovery message. Eventually returns a message containing the message type
     * RECOVERY_MSG_TYPE_COMPLETE indicating that all tuple data and updates to shipped data
     * have been sent. Returns false when there are no more recovery messages.
     */
    bool nextMessage(ReferenceSerializeOutput *out);

    /**
     * Mandatory TableStreamContext override.
     */
    virtual int64_t handleStreamMore(TupleOutputStreamProcessor &outputStreams,
                                     std::vector<int> &retPositions);

private:

    /**
     * Constructor - private so that only TableStreamer::activateStream() can call.
     */
    RecoveryContext(PersistentTable &table,
                    PersistentTableSurgeon &surgeon,
                    int32_t partitionId,
                    TupleSerializer &serializer,
                    int32_t tableId);

    bool m_firstMessage;
    /*
     * Iterator over the table. Iterator is configured
     * to scan every data block and ignore updates
     * during the iteration process
     */
    TableIterator m_iterator;

    /*
     * Integer indices of tuples that have been updated since being shipped
     * in a recovery message
     */
//    boost::unordered_set<uint32_t> m_updatedTupleIndices;
    /*
     * Not implemented yet, but a boost::unordered_set of the primary keys of tuples that were deleted
     * after being shipped in a recovery message
     */

    int32_t m_tableId;

    /*
     * Constants for message types can also be used to describe the current phase of
     * recovery.
     * Phase 1 is to ship tuples
     * Phase 2 is to ship updates
     * Phase 3 is to ship deletes
     */
    RecoveryMsgType m_recoveryPhase;

    DefaultTupleSerializer m_serializer;
};
}
#endif /* RECOVERYCONTEXT_H_ */
