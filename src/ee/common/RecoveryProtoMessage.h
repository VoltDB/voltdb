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

#ifndef RECOVERY_PROTO_MESSAGE_
#define RECOVERY_PROTO_MESSAGE_

#include "common/tabletuple.h"
#include "common/declarations.h"

namespace voltdb {
class Pool;

/*
 * A class for generating and receiving recovery messages. The class mixes read/write functionality along
 * with the ability to read/write several different types of payloads. RecoveryMsgType specifies the correct
 * set of methods that can be used with an instance of this class and fatal exceptions are thrown if the wrong
 * methods are used (mixing read/write, wrong method for payload type). Some recovery messages do not have a
 * data payload and consist solely of the RecoveryMsgType.
 *
 * Format is:
 * 1 byte message type
 * 4 byte table id
 * 4 byte tuple count
 * <tuples>
 *
 * The tuple count is omitted for some message types.
 */
class RecoveryProtoMsg {
public:

    /*
     * Prepare a recovery message for reading.
     */
    RecoveryProtoMsg(ReferenceSerializeInputBE *in);

    /*
     * Iterate over a recovery message and retrieve tuples for insertion/update.
     * Message type must be RECOVERY_MSG_TYPE_SCAN_TUPLES or RECOVERY_MSG_TYPE_DELTA_MERGE_TUPLES or
     * RECOVERY_MSG_TYPE_DELTA_DELETE_PKEYS
     * and the message must have been constructed with the read constructor
     */
    bool nextTuple(TableTuple &tuple, Pool *pool);

    /*
     * Retrieve the type of this recovery message.
     */
    RecoveryMsgType msgType();

    CatalogId tableId();

    /*
     * Number of tuples in the entire table (not just this message)
     */
    uint32_t totalTupleCount();

    ReferenceSerializeInputBE* stream();

private:
    /*
     * Input serializer.
     */
    ReferenceSerializeInputBE *m_in;

    /*
     * Type of this recovery message
     */
    RecoveryMsgType m_type;

    /*
     * Number of tuples already read from the message
     * via nextTuple
     */
    int32_t m_tuplesRead;

    int64_t m_exportStreamSeqNo;

    /*
     * CatalogId of the table this recovery message is for
     */
    CatalogId m_tableId;

    uint32_t m_totalTupleCount;
};
}
#endif //RECOVERY_PROTO_MESSAGE_
