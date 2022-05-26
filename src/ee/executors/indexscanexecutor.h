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

#include "common/tabletuple.h"
#include "executors/abstractexecutor.h"
#include "executors/OptimizedProjector.hpp"
#include "indexes/tableindex.h"

#include "boost/shared_array.hpp"

namespace voltdb {

class AbstractTempTable;
class AbstractExpression;
//
// Inline PlanNodes
//
class IndexScanPlanNode;
class ProjectionPlanNode;
class AggregateExecutorBase;
class InsertExecutor;
class CountingPostfilter;

class IndexScanExecutor : public AbstractExecutor {
    // Data in this class is arranged roughly in the order it is read for
    // p_execute(). Please don't reshuffle it only in the name of beauty.

    IndexScanPlanNode *m_node;
    int m_numOfSearchkeys;

    // Inline Projection
    ProjectionPlanNode* m_projectionNode;
    OptimizedProjector m_projector{};

    // Search key
    AbstractExpression** m_searchKeyArray;

    IndexLookupType m_lookupType;
    SortDirectionType m_sortDirection;
    bool m_hasOffsetRankOptimization;

    // IndexScan Information
    AbstractTempTable* m_outputTable;

    // arrange the memory mgmt aids at the bottom to try to maximize
    // cache hits (by keeping them out of the way of useful runtime data)
    boost::shared_array<AbstractExpression*> m_searchKeyArrayPtr;
    // So Valgrind doesn't complain:
    char* m_searchKeyBackingStore = nullptr;

    AggregateExecutorBase* m_aggExec = nullptr;
    InsertExecutor *m_insertExec = nullptr;

    bool p_init(AbstractPlanNode*, const ExecutorVector& executorVector);
    bool p_execute(const NValueArray &params);
    void outputTuple(CountingPostfilter& postfilter, TableTuple& tuple);
public:
    IndexScanExecutor(VoltDBEngine* engine, AbstractPlanNode* abstractNode)
        : AbstractExecutor(engine, abstractNode) {}
    ~IndexScanExecutor();

    /** This is a helper function to get the "next tuple" during an
     *   index scan, called by p_execute of both this class and
     *   NestLoopIndexExecutor. */
    static bool getNextTuple(
            IndexLookupType lookupType,
            TableTuple* tuple,
            TableIndex* index,
            IndexCursor* cursor,
            int activeNumOfSearchKeys) {
        if (lookupType == IndexLookupType::Equal || lookupType == IndexLookupType::GeoContains) {
            *tuple = index->nextValueAtKey(*cursor);
            if (! tuple->isNullTuple()) {
                return true;
            }
        }
        if ((lookupType != IndexLookupType::Equal && lookupType != IndexLookupType::GeoContains) ||
                activeNumOfSearchKeys == 0) {
            *tuple = index->nextValue(*cursor);
        }
        return ! tuple->isNullTuple();
    }
};

}

