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

#include "RankScanExecutor.h"

#include "common/debuglog.h"
#include "common/common.h"
#include "common/tabletuple.h"
#include "common/ValueFactory.hpp"
#include "expressions/abstractexpression.h"
#include "expressions/expressionutil.h"
#include "expressions/rankexpression.h"
#include "execution/ProgressMonitorProxy.h"
#include "executors/aggregateexecutor.h"
#include "indexes/tableindex.h"
#include "storage/table.h"
#include "storage/tableiterator.h"
#include "storage/temptable.h"
#include "storage/persistenttable.h"
#include "plannodes/aggregatenode.h"
#include "plannodes/rankscannode.h"
#include "plannodes/projectionnode.h"
#include "plannodes/limitnode.h"

using namespace voltdb;

bool RankScanExecutor::p_init(AbstractPlanNode *abstractNode,
                                TempTableLimits* limits)
{
    VOLT_DEBUG("init IndexCount Executor");

    m_node = dynamic_cast<RankScanPlanNode*>(abstractNode);
    assert(m_node);
    assert(m_node->getTargetTable());

    // Create output table based on output schema from the plan
    setTempOutputTable(limits);

    // Miscellanous Information
    m_lookupType = m_node->getLookupType();
    m_endType = m_node->getEndType();

    m_predicate = m_node->getPredicate();
    m_projectionNode = dynamic_cast<ProjectionPlanNode*>(m_node->getInlinePlanNode(PLAN_NODE_TYPE_PROJECTION));
    m_numOfColumns = -1;
    if (m_projectionNode != NULL) {
        m_numOfColumns = static_cast<int> (m_projectionNode->getOutputColumnExpressions().size());
    }

    //output table should be temptable
    m_outputTable = static_cast<TempTable*>(m_node->getOutputTable());

    // Inline aggregation can be serial, partial or hash
    m_aggExec = voltdb::getInlineAggregateExecutor(m_node);

    return true;
}

bool RankScanExecutor::p_execute(const NValueArray &params)
{
    // update local target table with its most recent reference
    RankExpression * rankExpr = m_node->getRankExpression();
    TableIndex * tableIndex = rankExpr->refreshGetTableIndex();

    if (rankExpr->getPartitonbySize() > 0) {
        throwDynamicSQLException("rank partition by clause is not supported");
    }

    LimitPlanNode* limitNode = dynamic_cast<LimitPlanNode*>(m_node->getInlinePlanNode(PLAN_NODE_TYPE_LIMIT));
    m_limit = -1;
    m_offset = -1;
    if (limitNode) {
        limitNode->getLimitAndOffsetByReference(params, m_limit, m_offset);
    }

    Table* input_table = m_node->getTargetTable();
    assert(input_table);

    Table* output_table = m_node->getOutputTable();
    assert(output_table);
    TempTable* output_temp_table = dynamic_cast<TempTable*>(output_table);
    ProgressMonitorProxy pmp(m_engine, this);
    if (m_aggExec != NULL) {
        const TupleSchema * inputSchema = input_table->schema();
        if (m_projectionNode != NULL) {
            inputSchema = m_projectionNode->getOutputTable()->schema();
        }
        m_tempTuple = m_aggExec->p_execute_init(params, &pmp,
                inputSchema, output_temp_table);
    } else {
        m_tempTuple = output_temp_table->tempTuple();
    }
    m_rkEnd = -1;
    m_rkOffset = -1;
    NValue rkKeyNvalue = m_node->getRankKeyExpression()->eval(NULL, NULL);
    m_rkStart = ValuePeeker::peekAsBigInt(rkKeyNvalue);

    if (m_lookupType == INDEX_LOOKUP_TYPE_GT) {
        m_rkStart++;
    }
    assert(m_lookupType == INDEX_LOOKUP_TYPE_GT ||
            m_lookupType == INDEX_LOOKUP_TYPE_EQ ||
            m_lookupType == INDEX_LOOKUP_TYPE_GTE);

    if (m_node->getEndExpression() != NULL) {
        NValue rkEndNvalue = m_node->getEndExpression()->eval(NULL, NULL);
        m_rkEnd = ValuePeeker::peekAsBigInt(rkEndNvalue);

        assert(m_endType == INDEX_LOOKUP_TYPE_LT || m_endType == INDEX_LOOKUP_TYPE_LTE);
        if (m_endType == INDEX_LOOKUP_TYPE_LTE) {
            m_rkEnd++;
        }
        m_rkOffset = m_rkEnd - m_rkStart;

        // no rows returned from the scan
        if (m_rkOffset <= 0 && m_aggExec != NULL)
            m_aggExec->p_execute_finish();
        return true;
    }


    IndexCursor indexCursor(tableIndex->getTupleSchema());
    TableTuple tuple(input_table->schema());
    bool found = tableIndex->findRankTuple(m_rkStart, indexCursor);
    if (m_lookupType == INDEX_LOOKUP_TYPE_EQ) {
        if (! found) {
            if (m_aggExec != NULL)
                m_aggExec->p_execute_finish();
            return true;
        }
        tuple = indexCursor.m_match;
        p_tryToInsertTuple(&tuple, pmp);
    }

    TableIterator iterator = input_table->iteratorDeletingAsWeGo();
    int tuple_ctr = 0, rank_ctr = 0;
    while ((m_limit == -1 || tuple_ctr < m_limit) &&
           ((m_lookupType == INDEX_LOOKUP_TYPE_EQ && tableIndex->isTheNextKeySame(indexCursor)) ||
            (m_lookupType != INDEX_LOOKUP_TYPE_EQ &&
             !(tuple = tableIndex->nextValue(indexCursor)).isNullTuple() &&
             (m_rkOffset = -1 || rank_ctr++ < m_rkOffset)) ))
    {
        RANK_INSERT_RESULT res = p_tryToInsertTuple(&tuple, pmp);

        if (res == RANK_INSERT_FAIL_ON_PREDICATE) {
            continue;
        }

        tuple_ctr++;

        if (res == RANK_INSERT_EALRY_RETURN_FROM_AGG) {
            break;
        }
    }

    if (m_aggExec != NULL) {
        m_aggExec->p_execute_finish();
    }

    VOLT_DEBUG("Finished RANK scanning");
    return true;
}

RANK_INSERT_RESULT RankScanExecutor::p_tryToInsertTuple(TableTuple* tuple, ProgressMonitorProxy &pmp) {
    assert(tuple != NULL);

    if (! (m_predicate == NULL || m_predicate->eval(tuple, NULL).isTrue())) {
        return RANK_INSERT_FAIL_ON_PREDICATE;
    }

    pmp.countdownProgress();

    //
    // Nested Projection
    // Project (or replace) values from input tuple
    //
    if (m_projectionNode != NULL)
    {
        VOLT_TRACE("inline projection...");
        for (int ctr = 0; ctr < m_numOfColumns; ctr++) {
            NValue value = m_projectionNode->getOutputColumnExpressions()[ctr]->eval(tuple, NULL);
            m_tempTuple.setNValue(ctr, value);
        }

        if (m_aggExec != NULL) {
            if (m_aggExec->p_execute_tuple(m_tempTuple)) {
                return RANK_INSERT_EALRY_RETURN_FROM_AGG;
            }
        } else {
            m_outputTable->insertTupleNonVirtual(m_tempTuple);
        }
    }
    else
    {
        if (m_aggExec != NULL) {
            if (m_aggExec->p_execute_tuple(*tuple)) {
                return RANK_INSERT_EALRY_RETURN_FROM_AGG;
            }
        } else {
            //
            // Insert the tuple into our output table
            //
            m_outputTable->insertTupleNonVirtual(*tuple);
        }
    }

    return RANK_INSERT_NORMAL_SUCCESS;
}



RankScanExecutor::~RankScanExecutor() {
}
