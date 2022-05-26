/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
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
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#pragma once

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
    virtual void deleteAllTuples() = 0;

    /** The temp table limits object for this table */
    virtual const TempTableLimits* getTempTableLimits() const = 0;

    /** Return a count of tuples in this table */
    virtual int64_t tempTableTupleCount() const { return m_tupleCount; }

    /**
     * Swap the tuples in this table with the tuples in another table
     */
    virtual void swapContents(AbstractTempTable* otherTable) {
        vassert(schema()->isCompatibleForMemcpy(otherTable->schema()));
        std::swap(m_tupleCount, otherTable->m_tupleCount);
        std::swap(m_tuplesPinnedByUndo, otherTable->m_tuplesPinnedByUndo);
        std::swap(m_nonInlinedMemorySize, otherTable->m_nonInlinedMemorySize);
    }

protected:
    AbstractTempTable(int tableAllocationTargetSize)
        : Table(tableAllocationTargetSize)
    {
    }
};

}

