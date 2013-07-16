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

#ifndef TABLE_STREAMER_CONTEXT_H
#define TABLE_STREAMER_CONTEXT_H

#include <vector>
#include <string>
#include <iostream>
#include "common/StreamPredicateList.h"
#include "common/FatalException.hpp"
#include "storage/TupleBlock.h"

namespace voltdb
{

class TupleOutputStreamProcessor;
class TableTuple;
class PersistentTable;

/**
 * Abstract class that provides the interface for all table streamer contexts.
 */
class TableStreamerContext {

public:

    virtual ~TableStreamerContext() {}

    /**
     * Mandatory streamMore() handler.
     */
    virtual int64_t handleStreamMore(TupleOutputStreamProcessor &outputStreams,
                                     std::vector<int> &retPositions) = 0;

    /**
     * Optional tuple insert handler.
     */
    virtual bool notifyTupleInsert(TableTuple &tuple) {return false;}

    /**
     * Optional tuple update handler.
     */
    virtual bool notifyTupleUpdate(TableTuple &tuple) {return false;}

    /**
     * Optional tuple delete handler.
     */
    virtual bool notifyTupleDelete(TableTuple &tuple) {return false;}

    /**
     * Optional block compaction handler.
     */
    virtual void notifyBlockWasCompactedAway(TBPtr block) {}

    /**
     * Optional tuple compaction handler.
     */
    virtual void notifyTupleMovement(TBPtr sourceBlock, TBPtr targetBlock,
                                     TableTuple &sourceTuple, TableTuple &targetTuple) {}

    /**
     * Optional tuple freeing check handler.
     */
    virtual bool canSafelyFreeTuple(TableTuple tuple) {return true;}

    /**
     * Table accessor.
     */
    PersistentTable &getTable()
    {
        return m_table;
    }

    /**
     * Predicates accessor.
     */
    StreamPredicateList &getPredicates()
    {
        return m_predicates;
    }

protected:

    /**
     * Constructor with predicates.
     */
    TableStreamerContext(PersistentTable &table, const std::vector<std::string> &predicateStrings) :
        m_table(table)
    {
        // Parse predicate strings. The factory type determines the kind of
        // predicates that get generated.
        // Throws an exception to be handled by caller on errors.
        std::ostringstream errmsg;
        if (!m_predicates.parseStrings(predicateStrings, errmsg, m_predicateDeleteFlags)) {
            throwFatalException("TableStreamerContext() failed to parse predicate strings.");
        }
    }

    /**
     * Constructor without predicates.
     */
    TableStreamerContext(PersistentTable &table) :
        m_table(table)
    {}

    /**
     * Predicate delete flags accessor.
     */
    std::vector<bool> &getPredicateDeleteFlags()
    {
        return m_predicateDeleteFlags;
    }

private:

    /**
     * Table being streamed.
     */
    PersistentTable &m_table;

    /**
     * Parsed hash range predicates.
     */
    StreamPredicateList m_predicates;

    /**
     * Per-predicate delete if true flags.
     */
    std::vector<bool> m_predicateDeleteFlags;
};

} // namespace voltdb

#endif // TABLE_STREAMER_CONTEXT_H
