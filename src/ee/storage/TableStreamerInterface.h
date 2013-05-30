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

#ifndef TABLE_STREAMER_INTERFACE_H
#define TABLE_STREAMER_INTERFACE_H

#include "common/types.h"
#include "storage/TupleBlock.h"

namespace voltdb
{
    class TupleOutputStreamProcessor;

    /**
     * Defines the interface for table streaming.
     */
    class TableStreamerInterface
    {
    public:

        virtual ~TableStreamerInterface()
        {}

        /**
         * Activate streaming.
         */
        virtual bool activateStream(PersistentTable &table, CatalogId tableId) = 0;

        /**
         * Continue streaming.
         */
        virtual int64_t streamMore(TupleOutputStreamProcessor &outputStreams,
                                   std::vector<int> &retPositions) = 0;

        /**
         * Return true if a tuple can be freed safely.
         */
        virtual bool canSafelyFreeTuple(TableTuple &tuple) const = 0;

        /**
         * Return true if the stream has already been activated.
         */
        virtual bool isAlreadyActive() const = 0;

        /**
         * Return the stream type, snapshot, recovery, etc..
         * TODO: Refactor so the caller doesn't need to know the stream type, just the context.
         */
        virtual TableStreamType getStreamType() const = 0;

        /**
         * Return the current active stream type or TABLE_STREAM_NONE if nothing is active.
         * TODO: Refactor so the caller doesn't need to know the stream type, just the context.
         */
        virtual TableStreamType getActiveStreamType() const = 0;

        /**
         * Tuple insert hook.
         * Return true if it was handled by the COW context.
         */
        virtual bool notifyTupleInsert(TableTuple &tuple) = 0;

        /**
         * Tuple update hook.
         * Return true if it was handled by the COW context.
         */
        virtual bool notifyTupleUpdate(TableTuple &tuple) = 0;

        /**
         * Tuple delete hook.
         * Return true if it was handled by the COW context.
         */
        virtual bool notifyTupleDelete(TableTuple &tuple) = 0;

        /**
         * Block compaction hook.
         */
        virtual void notifyBlockWasCompactedAway(TBPtr block) = 0;

        /**
         * Called for each tuple moved.
         */
        virtual void notifyTupleMovement(TBPtr sourceBlock, TBPtr targetBlock,
                                         TableTuple &sourceTuple, TableTuple &targetTuple) = 0;

    };

} // namespace voltdb

#endif // TABLE_STREAMER_INTERFACE_H
