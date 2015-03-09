/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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


#ifndef HSTOREINDEXCOUNTEXECUTOR_H
#define HSTOREINDEXCOUNTEXECUTOR_H

#include "common/common.h"
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
struct IndexCursor;

class IndexCountExecutor : public AbstractExecutor
{
public:
    IndexCountExecutor(VoltDBEngine* engine, AbstractPlanNode* abstractNode)
        : AbstractExecutor(engine, abstractNode), m_searchKeyBackingStore(NULL), m_endKeyBackingStore(NULL)
    {
    }
    ~IndexCountExecutor();

private:
    bool p_init(AbstractPlanNode*, TempTableLimits* limits);
    bool p_execute(const NValueArray &params);

    // Data in this class is arranged roughly in the order it is read for
    // p_execute(). Please don't reshuffle it only in the name of beauty.

    IndexCountPlanNode *m_node;
    int m_numOfColumns;
    int m_numOfSearchkeys;
    int m_numOfEndkeys;

    // Search key
    AbstractExpression** m_searchKeyArray;
    AbstractExpression** m_endKeyArray;

    IndexLookupType m_lookupType;
    IndexLookupType m_endType;

    // IndexCount Information
    TempTable* m_outputTable;


    // arrange the memory mgmt aids at the bottom to try to maximize
    // cache hits (by keeping them out of the way of useful runtime data)
    boost::shared_array<AbstractExpression*> m_searchKeyArrayPtr;
    boost::shared_array<AbstractExpression*> m_endKeyArrayPtr;

    // So Valgrind doesn't complain:
    char* m_searchKeyBackingStore;
    char* m_endKeyBackingStore;
};

}

#endif // HSTOREINDEXCOUNTEXECUTOR_H
