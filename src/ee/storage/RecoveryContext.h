/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
#ifndef RECOVERYCONTEXT_H_
#define RECOVERYCONTEXT_H_

#include "storage/tableiterator.h"
#include "common/DefaultTupleSerializer.h"

/*
 * A log of changes to tuple data that has already been sent to a recovering
 * partition as well as a mechanism to send messages containing recovery data.
 */
namespace voltdb {
class PersistentTable;
class ReferenceSerializeOutput;

class RecoveryContext {
public:
    RecoveryContext(PersistentTable *table, int32_t tableId);

    /*
     * Generate the next recovery message. Eventually returns a message containing the message type
     * RECOVERY_MSG_TYPE_COMPLETE indicating that all tuple data and updates to shipped data
     * have been sent. Returns false when there are no more recovery messages.
     */
    bool nextMessage(ReferenceSerializeOutput *out);
private:
    /*
     * Table that is the source of the recovery data
     */
    PersistentTable *m_table;

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
