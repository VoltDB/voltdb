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

#ifndef RECOVERY_PROTO_MESSAGE_BUILDER_
#define RECOVERY_PROTO_MESSAGE_BUILDER_

#include "common/types.h"
#include "common/tabletuple.h"

namespace voltdb {
class Pool;
class TupleSchema;
class TupleSerializer;
class ReferenceSerializeOutput;

/*
 * A class for generating and receiving recovery messages. The class mixes read/write functionality along
 * with the ability to read/write several different types of payloads. RecoveryMsgType specifies the correct
 * set of methods that can be used with an instance of this class and fatal exceptions are thrown if the wrong
 * methods are used (mixing read/write, wrong method for payload type). Some recovery messages do not have a
 * data payload and consist solely of the RecoveryMsgType.
 *
 * Format is:
 * 1 byte message type
 * 4 byte tuple count
 * <tuples>
 *
 * The tuple count is omitted for some message types.
 */
class RecoveryProtoMsgBuilder {
public:
    /*
     * Construct a recovery message to populate with recovery data
     */
    RecoveryProtoMsgBuilder(
            const RecoveryMsgType type,
            CatalogId tableId,
            uint32_t totalTupleCount,//Number of tuples in table overall
                                    //Not the number in this message. Used to size hash tables.
            ReferenceSerializeOutput *out,
            TupleSerializer *serializer,
            const TupleSchema *schema);

    /*
     * Return true if another max size tuple can fit
     */
    bool canAddMoreTuples();

    /*
     * Add a tuple to be inserted or updated at the recovering partition.
     */
    void addTuple(TableTuple tuple);

    /*
     * Write the tuple count and any other information
     */
    void finalize();

private:
    /*
     * Output serializer. May be null if this is a received message
     */
    ReferenceSerializeOutput *m_out;

    /*
     * Position to put the count of tuples @ once serialization is complete.
     */
    size_t m_tupleCountPosition;

    /*
     * Counter of tuples add or or tuple count extracted from message
     */
    int32_t m_tupleCount;

    int32_t m_maxSerializedSize;
};
}
#endif //RECOVERY_PROTO_MESSAGE_BUILDER_
