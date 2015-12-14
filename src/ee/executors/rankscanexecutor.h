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


#ifndef HSTORERANKSCANEXECUTOR_H
#define HSTORERANKSCANEXECUTOR_H

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
class AbstractExpression;
class RankScanPlanNode;
class ProgressMonitorProxy;
class ProjectionPlanNode;
class AggregateExecutorBase;

enum RANK_INSERT_RESULT {
    RANK_INSERT_FAIL_ON_PREDICATE = -1,
    RANK_INSERT_EALRY_RETURN_FROM_AGG = 0,
    RANK_INSERT_NORMAL_SUCCESS = 1
};

class RankScanExecutor : public AbstractExecutor
{
public:
    RankScanExecutor(VoltDBEngine* engine, AbstractPlanNode* abstractNode)
        : AbstractExecutor(engine, abstractNode)
    {
    }
    ~RankScanExecutor();

private:
    bool p_init(AbstractPlanNode*, TempTableLimits* limits);
    bool p_execute(const NValueArray &params);



    RANK_INSERT_RESULT p_tryToInsertTuple(TableTuple* tuple, ProgressMonitorProxy &pmp);

    // Data in this class is arranged roughly in the order it is read for
    // p_execute(). Please don't reshuffle it only in the name of beauty.

    RankScanPlanNode *m_node;
    IndexLookupType m_lookupType;
    IndexLookupType m_endType;

    int64_t m_rkStart;
    int64_t m_rkEnd;
    int64_t m_rkOffset;

    AbstractExpression * m_predicate;
    ProjectionPlanNode* m_projectionNode;
    int m_numOfColumns;

    int m_limit;
    int m_offset;

    TableTuple m_tempTuple;
    TempTable* m_outputTable;

    AggregateExecutorBase* m_aggExec;
};

}

#endif // HSTOREINDEXCOUNTEXECUTOR_H
