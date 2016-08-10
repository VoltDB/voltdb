/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
#include "IndexCopyOnWriteContext.h"

#include "indexes/tableindex.h"
#include "indexes/tableindexfactory.h"
#include "storage/temptable.h"
#include "storage/tablefactory.h"
#include "storage/CopyOnWriteIterator.h"
#include "storage/persistenttable.h"
#include "storage/tableiterator.h"
#include "common/TupleOutputStream.h"
#include "common/FatalException.hpp"
#include "logging/LogManager.h"
#include <algorithm>
#include <cassert>
#include <iostream>

namespace voltdb {

/**
 * Constructor.
 */
IndexCopyOnWriteContext::IndexCopyOnWriteContext(
        PersistentTable &table,
        PersistentTableSurgeon &surgeon,
        TableIndex &index,
        int32_t partitionId,
        int64_t totalTuples) :
                TableStreamerContext(table, surgeon, partitionId),
             m_backedUpTuples(TableFactory::buildCopiedTempTable("COW of " + table.name() + " " + index.getName(),
                                                                 &table, NULL)),
             m_table(table),
             m_surgeon(surgeon),
             m_index(index),
             m_indexCursor(index.getTupleSchema()),
             m_deletesCursor(index.getTupleSchema()),
             m_pool(2097152, 320),
             m_lastIndexTuple(table.schema()),
             m_lastDeletesTuple(table.schema()),
             m_finished(false),
             m_indexLookupType(INDEX_LOOKUP_TYPE_INVALID),
             m_inserts(0),
             m_deletes(0),
             m_updates(0)
{
    voltdb::TableIndexScheme indexScheme(index.getScheme());
    indexScheme.negativeDelta = true;
    indexScheme.countable = false;
    m_indexInserts = TableIndexFactory::getInstance(indexScheme);
    m_indexDeletes = TableIndexFactory::getInstance(indexScheme);
}

/**
 * Destructor.
 */
IndexCopyOnWriteContext::~IndexCopyOnWriteContext()
{}

/**
 * Activation handler.
 */
TableStreamerContext::ActivationReturnCode
IndexCopyOnWriteContext::handleActivation(TableStreamType streamType)
{
    if (m_finished) {
        return ACTIVATION_FAILED;
    }
    //m_surgeon.activateSnapshot();
    return ACTIVATION_SUCCEEDED;
}

// Arrange cursors to point towards the first tuple that we have not read yet
bool
IndexCopyOnWriteContext::adjustCursors(int type, IndexCursor *cursor) {

    if (m_indexLookupType == INDEX_LOOKUP_TYPE_INVALID) {
        m_indexLookupType = static_cast<IndexLookupType>(type);
    }

    //* debug */ std::cout << "IndexCopyOnWriteContext::adjustCursors " << type << std::endl;
    //* debug */ debug();

    if (m_lastIndexTuple.isNullTuple() && m_lastDeletesTuple.isNullTuple() && cursor != NULL) {
        m_indexCursor = *cursor;
        return true;
    }

    TableTuple tuple;

    if (m_indexLookupType == INDEX_LOOKUP_TYPE_EQ ||
            m_indexLookupType == INDEX_LOOKUP_TYPE_GT ||
            m_indexLookupType == INDEX_LOOKUP_TYPE_GTE
            ) {
        if (m_lastIndexTuple.isNullTuple()) {
            m_index.moveToEnd(true, m_indexCursor);
        }
        else {
            m_index.moveToGreaterThanKeyByTuple(&m_lastIndexTuple, m_indexCursor);
        }
        if (m_lastDeletesTuple.isNullTuple()) {
            m_indexDeletes->moveToEnd(true, m_deletesCursor);
        }
        else {
            m_indexDeletes->moveToKeyByTupleAddr(&m_lastDeletesTuple, m_lastDeletesTupleAddr, m_deletesCursor);
            m_deletesCursor.m_forward = true;
            m_indexDeletes->nextValue(m_deletesCursor);
        }

    }
    else if (m_indexLookupType == INDEX_LOOKUP_TYPE_LT ||
            m_indexLookupType == INDEX_LOOKUP_TYPE_LTE
            ) {
        if (m_lastIndexTuple.isNullTuple()) {
            m_index.moveToEnd(false, m_indexCursor);
        }
        else {
            m_index.moveToLessThanKeyByTuple(&m_lastIndexTuple, m_indexCursor);
        }
        if (m_lastDeletesTuple.isNullTuple()) {
            m_indexDeletes->moveToEnd(false, m_deletesCursor);
        }
        else {
            m_indexDeletes->moveToKeyByTupleAddr(&m_lastDeletesTuple, m_lastDeletesTupleAddr, m_deletesCursor);
            m_deletesCursor.m_forward = false;
            m_indexDeletes->nextValue(m_deletesCursor);
        }
    }
    else if (m_indexLookupType == INDEX_LOOKUP_TYPE_GEO_CONTAINS) {
        // moveToCoveringCell  finds the exact tuple... so we need a way to find the next one m_tuple should be at
        // also, the exact tuple will either be in m_index or m_indexDeletes, but not both, so we need to find
        // a way to get to the "next" value
        m_index.moveToCoveringCell(&m_lastIndexTuple, m_indexCursor);
        m_indexDeletes->moveToCoveringCell(&m_lastDeletesTuple, m_deletesCursor);
    }
    return true;
}

/**
 * Advance the COW iterator and return the next tuple
 */
bool IndexCopyOnWriteContext::advanceIterator(TableTuple &tuple) {
    PersistentTable &table = m_table;
    //* debug */ std::cout << "advanceIterator "<< std::endl;
    //* debug */ std::cout << "INDEX " << m_index.debug() << std::endl;
    //* debug */ std::cout << "INSERTS " << m_indexInserts->debug() << std::endl;
    //* debug */ std::cout << "DELETES " << m_indexDeletes->debug() << std::endl;
    // Compare cursors and start from the lowest between deletes and current index
    TableTuple deletesTuple(table.schema());
    TableTuple indexTuple(table.schema());

    if (m_indexLookupType == INDEX_LOOKUP_TYPE_EQ) {
        deletesTuple = m_indexDeletes->currentValueAtKey(m_deletesCursor);
        indexTuple = m_index.currentValueAtKey(m_indexCursor);
    }
    else {
        deletesTuple = m_indexDeletes->currentValue(m_deletesCursor);
        indexTuple = m_index.currentValue(m_indexCursor);
    }

    while (!indexTuple.isNullTuple() || !deletesTuple.isNullTuple()) {
        bool deleteTupleLessThanindexTuple = m_indexDeletes->compare(&indexTuple, m_deletesCursor) > 0;
        if (!deletesTuple.isNullTuple() &&
                (indexTuple.isNullTuple() ||
                ((m_indexLookupType == INDEX_LOOKUP_TYPE_EQ ||
                        m_indexLookupType == INDEX_LOOKUP_TYPE_GT ||
                        m_indexLookupType == INDEX_LOOKUP_TYPE_GTE)
                        && deleteTupleLessThanindexTuple) ||
                ((m_indexLookupType == INDEX_LOOKUP_TYPE_LT ||
                        m_indexLookupType == INDEX_LOOKUP_TYPE_LTE)
                        && !deleteTupleLessThanindexTuple))
                ) {
            m_lastDeletesTuple = deletesTuple;
            m_lastDeletesTupleAddr = m_indexDeletes->currentKey(m_deletesCursor);
            // found the next tuple to scan in the delete records...return it
            if (m_indexLookupType == INDEX_LOOKUP_TYPE_EQ
                    || m_indexLookupType == INDEX_LOOKUP_TYPE_GEO_CONTAINS) {
                deletesTuple = m_indexDeletes->nextValueAtKey(m_deletesCursor);
            }
            else {
                deletesTuple = m_indexDeletes->nextValue(m_deletesCursor);
            }
            tuple = m_lastDeletesTuple;
            return true;
        }
        else {
            // found the next tuple to scan in the normal index.
            // check if this tuple can be found in the insert keys
            m_lastIndexTuple = indexTuple;
            // Move the cursor to the next value in the sequence
            if (m_indexLookupType == INDEX_LOOKUP_TYPE_EQ
                    || m_indexLookupType == INDEX_LOOKUP_TYPE_GEO_CONTAINS) {
                indexTuple = m_index.nextValueAtKey(m_indexCursor);
            }
            else {
                indexTuple = m_index.nextValue(m_indexCursor);
            }
            if (m_indexInserts->exists(&m_lastIndexTuple)) {
                // Set the index tuple to the next unread value

                if (m_indexLookupType == INDEX_LOOKUP_TYPE_EQ
                        || m_indexLookupType == INDEX_LOOKUP_TYPE_GEO_CONTAINS) {
                    indexTuple = m_index.currentValueAtKey(m_indexCursor);
                }
                else {
                    indexTuple = m_index.currentValue(m_indexCursor);
                }

                continue;
            }
            tuple = m_lastIndexTuple;
            return true;
        }

        break;
    }
    m_finished = true;
    return false;

}

/**
 * Returns true for success, false if there was a serialization error
 */
bool IndexCopyOnWriteContext::cleanup() {
    return true;
}

bool IndexCopyOnWriteContext::notifyTupleDelete(TableTuple &tuple) {
    //* debug */ std::cout << "notifyTupleDelete " << tuple.debugNoHeader() << std::endl;
    PersistentTable &table = m_table;
    TableTuple conflict(table.schema());
    TableTuple copy(table.schema());

    m_deletes++;

    if (!m_indexInserts->exists(&tuple)) {
        // Copy data
        copy = m_backedUpTuples->insertTempTupleDeepCopy(tuple, &m_pool);
        // Add to delete tree
        m_indexDeletes->addEntryNegativeDelta(&copy, tuple.address());


        // We may need to adjust the delete cursor
        if (!m_lastIndexTuple.isNullTuple()) {
            // is the tuple updated before the lastIndexTuple?

            int tupleLTELastIndexCompare = m_index.compare(&tuple, &m_lastIndexTuple);

            bool deleteTupleLessThanindexTuple = false;
            if (!m_lastDeletesTuple.isNullTuple()) {

                m_indexDeletes->moveToKeyByTupleAddr(&m_lastDeletesTuple, m_lastDeletesTupleAddr, m_deletesCursor);
                int deletesComp = m_indexDeletes->compare(&tuple, m_deletesCursor);
                deleteTupleLessThanindexTuple = deletesComp > 0;
            }
            bool typeGT = (m_indexLookupType == INDEX_LOOKUP_TYPE_EQ ||
                    m_indexLookupType == INDEX_LOOKUP_TYPE_GT ||
                    m_indexLookupType == INDEX_LOOKUP_TYPE_GTE);
            bool typeLT = (m_indexLookupType == INDEX_LOOKUP_TYPE_LT ||
                    m_indexLookupType == INDEX_LOOKUP_TYPE_LTE);
            if (
                    (typeGT
                    && (m_lastDeletesTuple.isNullTuple() || deleteTupleLessThanindexTuple)
                    && tupleLTELastIndexCompare <= 0) ||
                    (typeLT
                    && (m_lastDeletesTuple.isNullTuple() || !deleteTupleLessThanindexTuple)
                    && tupleLTELastIndexCompare >= 0)) {

                m_lastDeletesTuple = copy;
                m_lastDeletesTupleAddr = tuple.address();

            }
        }

    }
    else {
        m_indexInserts->deleteEntry(&tuple);
    }

    if (!m_lastIndexTuple.isNullTuple() && m_index.compare(&tuple, &m_lastIndexTuple) == 0) {
        // need to readjust index cursor so m_lastIndexTuple points to a valid tuple after this one is deleted
        if (m_indexLookupType == INDEX_LOOKUP_TYPE_EQ ||
                m_indexLookupType == INDEX_LOOKUP_TYPE_GT ||
                m_indexLookupType == INDEX_LOOKUP_TYPE_GTE
                ) {
            m_index.moveToLessThanKeyByTuple(&m_lastIndexTuple, m_indexCursor);
        }
        else if (m_indexLookupType == INDEX_LOOKUP_TYPE_LT ||
                m_indexLookupType == INDEX_LOOKUP_TYPE_LTE) {
            m_index.moveToGreaterThanKeyByTuple(&m_lastIndexTuple, m_indexCursor);
        }

        m_lastIndexTuple = m_index.currentValue(m_indexCursor);


    }
    //* debug */ debug();

    return true;
}

void IndexCopyOnWriteContext::notifyBlockWasCompactedAway(TBPtr block) {
    // TODO: Compaction may affect Inserts Index since tuple addresses may change
    return;
}

bool IndexCopyOnWriteContext::notifyTupleInsert(TableTuple &tuple) {
    //* debug */ std::cout << "notifyTupleInsert " << tuple.debugNoHeader() << std::endl;
    PersistentTable &table = m_table;
    TableTuple conflict(table.schema());
    // Add to insert tree
    m_indexInserts->addEntryNegativeDelta(&tuple, tuple.address());

    //* debug */ debug();

    return true;
}

bool IndexCopyOnWriteContext::notifyTupleUpdate(TableTuple &tuple) {
    PersistentTable &table = m_table;
    TableTuple conflict(table.schema());
    //* debug */ std::cout << "notifyTupleUpdate " << tuple.debugNoHeader() << std::endl;
    if (!m_indexInserts->exists(&tuple)) {
        // Copy data
        TableTuple copy(table.schema());
        copy = m_backedUpTuples->insertTempTupleDeepCopy(tuple, &m_pool);
        // Add to delete tree
        m_indexDeletes->addEntryNegativeDelta(&copy, tuple.address());

        // We may need to adjust the delete cursor
        if (!m_lastIndexTuple.isNullTuple()) {
            // is the tuple updated before the lastIndexTuple?
            //m_index.moveToKeyByTuple(&m_lastIndexTuple, m_indexCursor);
            //m_index.moveToKeyByTuple(&m_lastIndexTuple, m_indexCursor);
            int tupleLTELastIndexCompare = m_index.compare(&tuple, &m_lastIndexTuple);

            bool deleteTupleLessThanindexTuple = false;
            if (!m_lastDeletesTuple.isNullTuple()) {
                m_indexDeletes->moveToKeyByTupleAddr(&m_lastDeletesTuple, m_lastDeletesTupleAddr, m_deletesCursor);
                deleteTupleLessThanindexTuple = m_indexDeletes->compare(&tuple, m_deletesCursor) > 0;
            }
            bool typeGT = (m_indexLookupType == INDEX_LOOKUP_TYPE_EQ ||
                    m_indexLookupType == INDEX_LOOKUP_TYPE_GT ||
                    m_indexLookupType == INDEX_LOOKUP_TYPE_GTE);
            bool typeLT = (m_indexLookupType == INDEX_LOOKUP_TYPE_LT ||
                    m_indexLookupType == INDEX_LOOKUP_TYPE_LTE);
            if (
                    (typeGT
                    && (m_lastDeletesTuple.isNullTuple() || deleteTupleLessThanindexTuple)
                    && tupleLTELastIndexCompare <= 0) ||
                    (typeLT
                    && (m_lastDeletesTuple.isNullTuple() || !deleteTupleLessThanindexTuple)
                    && tupleLTELastIndexCompare >= 0)) {

                m_lastDeletesTuple = copy;
                m_lastDeletesTupleAddr = tuple.address();
            }
        }

    }
    m_indexInserts->deleteEntry(&tuple);

    if (!m_lastIndexTuple.isNullTuple() && m_index.compare(&tuple, &m_lastIndexTuple) == 0) {
        // need to readjust index cursor so m_lastIndexTuple points to a valid tuple after this one is deleted
        if (m_indexLookupType == INDEX_LOOKUP_TYPE_EQ ||
                m_indexLookupType == INDEX_LOOKUP_TYPE_GT ||
                m_indexLookupType == INDEX_LOOKUP_TYPE_GTE
                ) {
            m_index.moveToLessThanKeyByTuple(&m_lastIndexTuple, m_indexCursor);
        }
        else if (m_indexLookupType == INDEX_LOOKUP_TYPE_LT ||
                m_indexLookupType == INDEX_LOOKUP_TYPE_LTE) {
            m_index.moveToGreaterThanKeyByTuple(&m_lastIndexTuple, m_indexCursor);
        }

        m_lastIndexTuple = m_index.currentValue(m_indexCursor);

    }

    //* debug */ debug();

    return true;
}

bool IndexCopyOnWriteContext::notifyTuplePostUpdate(TableTuple &tuple) {
    //* debug */ std::cout << "notifyTuplePostUpdate " << tuple.debugNoHeader() << std::endl;
    PersistentTable &table = m_table;
    TableTuple conflict(table.schema());
    // Add new to insert tree
    m_indexInserts->addEntryNegativeDelta(&tuple, tuple.address());

    //* debug */ debug();

    return true;
}

void IndexCopyOnWriteContext::debug() {
    std::cout << "INDEX " << m_index.debug() << std::endl;
    std::cout << "INSERTS " << m_indexInserts->debug() << std::endl;
    std::cout << "DELETES " << m_indexDeletes->debug() << std::endl;

    std::cout << "m_lastIndexTuple ";
    if (!m_lastIndexTuple.isNullTuple()) {
        std::cout << m_lastIndexTuple.debugNoHeader();
    }
    std::cout << std::endl;
    std::cout << "m_lastDeletesTuple ";
    if (!m_lastDeletesTuple.isNullTuple()) {
        std::cout << m_lastDeletesTuple.debugNoHeader();
    }
    std::cout << std::endl;
}

}
