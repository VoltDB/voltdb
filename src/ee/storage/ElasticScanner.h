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

#ifndef ELASTICSCANNER_H_
#define ELASTICSCANNER_H_

#include <boost/shared_ptr.hpp>
#include "storage/TupleIterator.h"
#include "storage/TupleBlock.h"

namespace voltdb
{

class PersistentTable;
class TableTuple;

namespace elastic
{

class ScannerFactory;

/**
 * Tuple iterator that can perform a complete table scan even while mutations
 * are happening.
 */
class Scanner : public TupleIterator
{
    friend class ScannerFactory;

  public:

    /**
     * Destructor.
     */
    virtual ~Scanner() {}

    /**
     * Required TupleIterator override.
     * Get the next tuple or return false if none is available.
     */
    virtual bool next(TableTuple &out);

    /**
     * Block compaction hook.
     */
    void notifyBlockWasCompactedAway(TBPtr block);

    /**
     * Tuple insert hook.
     */
    void notifyTupleInsert(TableTuple &tuple);

    /**
     * Tuple update hook.
     */
    void notifyTupleUpdate(TableTuple &tuple);

  private:

    /**
     * Private constructor used by ScannerFactory.
     */
    Scanner(PersistentTable &table);

    /**
     * Internal method that handles transitions between blocks and
     * returns true as long as tuples are available.
     */
    bool continueScan();

    /// Table being iterated.
    PersistentTable &m_table;

    /// Block map.
    TBMap m_blockMap;

    /// Tuple size in bytes.
    const int m_tupleSize;

    /// Block iterator.
    TBMapI m_blockIterator;

    /// Block iterator end marker.
    TBMapI m_blockEnd;

    /// Current block pointer.
    TBPtr m_currentBlockPtr;

    /// Current tuple pointer.
    char *m_tuplePtr;

    /// Current tuple index (0-n within the block).
    uint32_t m_tupleIndex;
};

/**
 * In order to avoid dependency cycles classes that want to create scanners
 * must inherit from this class.
 */
class ScannerFactory
{

  public:

    /**
     * Create a new elastic row scanner.
     */
    static boost::shared_ptr<Scanner> makeScanner(PersistentTable &table) {
        return boost::shared_ptr<Scanner>(new Scanner(table));
    }

};

} // namespace elastic
} // namespace voltdb

#endif // ELASTICSCANNER_H_
