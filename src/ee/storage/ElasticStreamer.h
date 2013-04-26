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

#ifndef TABLE_STREAM_H
#define TABLE_STREAM_H

#include <string>
#include <vector>
#include <list>
#include <boost/scoped_ptr.hpp>
#include "common/ids.h"
#include "common/types.h"
#include "common/TupleSerializer.h"
#include "storage/TupleBlock.h"
#include "storage/ElasticScanner.h"

namespace voltdb
{

class CopyOnWriteContext;
class RecoveryContext;
class ReferenceSerializeInput;
class PersistentTable;
class TupleOutputStreamProcessor;

namespace elastic
{

class Streamer
{
public:

    /**
     * Constructor with data from serialized message.
     */
    Streamer(TupleSerializer &tupleSerializer,
             TableStreamType streamType,
             int32_t partitionId,
             ReferenceSerializeInput &serializeIn);

    /**
     * Destructor.
     */
    virtual ~Streamer();

    /**
     * Return true if the stream has already been activated.
     */
    bool isAlreadyActive() const;

    /**
     * Activate streaming.
     */
    bool activateStream(PersistentTable &table, CatalogId tableId);

    /**
     * Continue streaming.
     */
    int64_t streamMore(TupleOutputStreamProcessor &outputStreams, std::vector<int> &retPositions);

    /**
     * Block compaction hook.
     */
    void notifyBlockWasCompactedAway(TBPtr block);

    /**
     * Tuple insert hook.
     * Return true if it was handled by the COW context.
     */
    bool notifyTupleInsert(TableTuple &tuple);

    /**
     * Tuple update hook.
     * Return true if it was handled by the COW context.
     */
    bool notifyTupleUpdate(TableTuple &tuple);

    /**
     * Return true if recovery is in progress.
     */
    bool isCopyOnWriteActive() const {
        return m_COWContext.get() != NULL;
    }

    /**
     * Return the stream type, snapshot, recovery, etc..
     */
    TableStreamType getStreamType() const {
        return m_streamType;
    }

    /**
     * Return true if recovery is in progress.
     */
    bool isRecoveryActive() const {
        return m_recoveryContext.get() != NULL;
    }

    /**
     * Return true if a tuple can be freed safely.
     */
    bool canSafelyFreeTuple(TableTuple &tuple) const;

    /**
     * Create new elastic row scanner.
     */
    boost::shared_ptr<Scanner> makeScanner(PersistentTable &table);

    /**
     * Delete scanner produced by makeScanner().
     */
    void deleteScanner(Scanner *scanner);

private:

    /// Tuple serializer.
    TupleSerializer &m_tupleSerializer;

    /// The type of scan.
    const TableStreamType m_streamType;

    /// Current partition ID.
    int32_t m_partitionId;

    /// Predicate strings.
    std::vector<std::string> m_predicateStrings;

    /// True if rows should be deleted after streaming.
    bool m_doDelete;

    /// Context to keep track of snapshot scans.
    boost::scoped_ptr<CopyOnWriteContext> m_COWContext;

    /// Context to keep track of recovery scans.
    boost::scoped_ptr<RecoveryContext> m_recoveryContext;

    /// List of active scanners to keep updated.
    std::list<boost::shared_ptr<Scanner> > m_scanners;
};


} // namespace elastic
} // namespace voltdb

#endif // TABLE_STREAM_H
