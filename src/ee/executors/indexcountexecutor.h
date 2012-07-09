/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

#ifndef HSTOREINDEXCOUNTEXECUTOR_H
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
class IndexCountPlanNode;

class IndexCountExecutor : public AbstractExecutor
{
public:
    IndexCountExecutor(VoltDBEngine* engine, AbstractPlanNode* abstractNode)
        : AbstractExecutor(engine, abstractNode), m_searchKeyBackingStore(NULL)
    {
    }
    ~IndexCountExecutor();

protected:
    bool p_init(AbstractPlanNode*,
            TempTableLimits* limits);
    bool p_execute(const NValueArray &params);

    // Data in this class is arranged roughly in the order it is read for
    // p_execute(). Please don't reshuffle it only in the name of beauty.

    IndexCountPlanNode *m_node;
    int m_numOfColumns;
    int m_numOfSearchkeys;

    // Search key
    TableTuple m_searchKey;
    // search_key_beforesubstitute_array_ptr[]
    AbstractExpression** m_searchKeyBeforeSubstituteArray;
    bool* m_needsSubstituteSearchKey; // needs_substitute_search_key_ptr[]
    bool m_needsSubstitutePostExpression;
    bool m_needsSubstituteEndExpression;

    IndexLookupType m_lookupType;

    // IndexCount Information
    TempTable* m_outputTable;
    PersistentTable* m_targetTable;

    TableIndex *m_index;
    TableTuple m_dummy;
    TableTuple m_tuple;

    // arrange the memory mgmt aids at the bottom to try to maximize
    // cache hits (by keeping them out of the way of useful runtime data)
    boost::shared_array<bool> m_needsSubstituteSearchKeyPtr;
    boost::shared_array<AbstractExpression*>
        m_searchKeyBeforeSubstituteArrayPtr;
    // So Valgrind doesn't complain:
    char* m_searchKeyBackingStore;
};

}

#endif
