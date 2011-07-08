/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
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

#ifndef HSTOREINDEXSCANEXECUTOR_H
#define HSTOREINDEXSCANEXECUTOR_H

#include "common/valuevector.h"
#include "common/tabletuple.h"
#include "executors/abstractexecutor.h"

#include "boost/shared_array.hpp"
#include "boost/unordered_set.hpp"
#include "boost/pool/pool_alloc.hpp"
#include <set>
#include <memory>

namespace voltdb {

class TempTable;
class PersistentTable;

class AbstractExpression;

//
// Inline PlanNodes
//
class IndexScanPlanNode;
class ProjectionPlanNode;
class LimitPlanNode;

class IndexScanExecutor : public AbstractExecutor
{
public:
    IndexScanExecutor(VoltDBEngine* engine, AbstractPlanNode* abstractNode)
        : AbstractExecutor(engine, abstractNode), m_searchKeyBackingStore(NULL)
    {
        m_projectionExpressions = NULL;
    }
    ~IndexScanExecutor();

protected:
    bool p_init(AbstractPlanNode*,
                TempTableLimits* limits);
    bool p_execute(const NValueArray &params);

    // Data in this class is arranged roughly in the order it is read for
    // p_execute(). Please don't reshuffle it only in the name of beauty.

    IndexScanPlanNode *m_node;
    int m_numOfColumns;
    int m_numOfSearchkeys;

    // Inline Projection
    ProjectionPlanNode* m_projectionNode;
    int* m_projectionAllTupleArray; // projection_all_tuple_array_ptr[]
    AbstractExpression** m_projectionExpressions;

    // Search key
    TableTuple m_searchKey;
    // search_key_beforesubstitute_array_ptr[]
    AbstractExpression** m_searchKeyBeforeSubstituteArray;
    int* m_searchKeyAllParamArray; // search_key_all_param_array_ptr[]
    bool* m_needsSubstituteProject; // needs_substitute_project_ptr[]
    bool* m_needsSubstituteSearchKey; // needs_substitute_search_key_ptr[]
    bool m_needsSubstitutePostExpression;
    bool m_needsSubstituteEndExpression;

    IndexLookupType m_lookupType;
    SortDirectionType m_sortDirection;

    // Inline Limit
    LimitPlanNode* m_limitNode;
    int m_limitSize;
    int m_limitOffset;

    // IndexScan Information
    TempTable* m_outputTable;
    PersistentTable* m_targetTable;

    TableIndex *m_index;
    TableTuple m_dummy;
    TableTuple m_tuple;

    // arrange the memory mgmt aids at the bottom to try to maximize
    // cache hits (by keeping them out of the way of useful runtime data)
    boost::shared_array<bool> m_needsSubstituteSearchKeyPtr;
    boost::shared_array<bool> m_needsSubstituteProjectPtr;
    boost::shared_array<int> m_projectionAllTupleArrayPtr;
    boost::shared_array<AbstractExpression*>
        m_searchKeyBeforeSubstituteArrayPtr;
    boost::shared_array<int> m_searchKeyAllParamArrayPtr;
    // So Valgrind doesn't complain:
    char* m_searchKeyBackingStore;
};

}

#endif
