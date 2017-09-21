/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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

#ifndef _STORAGE_ABSTRACTTEMPTABLE_HPP
#define _STORAGE_ABSTRACTTEMPTABLE_HPP

#include "storage/table.h"

namespace voltdb {
/**
 * An abstract base class whose concrete implementations are
 * TempTable (for normal workload) and LargeTempTable (for
 * queries that need to page data to disk)
 */
class AbstractTempTable : public Table {
public:
    /** insert a tuple */
    virtual void insertTempTuple(TableTuple &source) = 0;

    /** Mark this table as no longer being inserted into */
    virtual void finishInserts() = 0;

    /** Delete all tuples in this table (done when fragment execution
        is complete) */
    virtual void deleteAllTempTuples() = 0;

    /** The temp table limits object for this table */
    virtual const TempTableLimits* getTempTableLimits() const = 0;

    /** Return a count of tuples in this table */
    virtual int64_t tempTableTupleCount() const { return m_tupleCount; }

protected:
    AbstractTempTable(int tableAllocationTargetSize)
        : Table(tableAllocationTargetSize)
    {
    }
};

}

#endif // _STORAGE_ABSTRACTTEMPTABLE_HPP
