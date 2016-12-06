/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

#include "orderbyexecutor.h"
#include "common/debuglog.h"
#include "common/common.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "execution/ProgressMonitorProxy.h"
#include "plannodes/orderbynode.h"
#include "plannodes/limitnode.h"
#include "storage/table.h"
#include "storage/temptable.h"
#include "storage/tableiterator.h"
#include "storage/tablefactory.h"

#include <algorithm>
#include <vector>

using namespace voltdb;
using namespace std;

bool
OrderByExecutor::p_init(AbstractPlanNode* abstract_node,
                        TempTableLimits* limits)
{
    VOLT_TRACE("init OrderBy Executor");

    OrderByPlanNode* node = dynamic_cast<OrderByPlanNode*>(abstract_node);
    assert(node);

    if (!node->isInline()) {
        assert(node->getInputTableCount() == 1);

        assert(node->getChildren()[0] != NULL);

        //
        // Our output table should look exactly like our input table
        //
        node->setOutputTable(TableFactory::buildCopiedTempTable(node->getInputTable()->name(),
                                                                node->getInputTable(),
                                                                limits));
        // pickup an inlined limit, if one exists
        limit_node =
            dynamic_cast<LimitPlanNode*>(node->
                                     getInlinePlanNode(PLAN_NODE_TYPE_LIMIT));
    } else {
        assert(node->getChildren().empty());
        assert(node->getInlinePlanNode(PLAN_NODE_TYPE_LIMIT) == NULL);
    }

#if defined(VOLT_LOG_LEVEL)
#if VOLT_LOG_LEVEL<=VOLT_LEVEL_TRACE
    const std::vector<AbstractExpression*>& sortExprs = node->getSortExpressions();
    for (int i = 0; i < sortExprs.size(); ++i) {
        VOLT_TRACE("Sort key[%d]:\n%s", i, sortExprs[i]->debug(true).c_str());
    }
#endif
#endif

    return true;
}

bool
OrderByExecutor::p_execute(const NValueArray &params)
{
    OrderByPlanNode* node = dynamic_cast<OrderByPlanNode*>(m_abstractNode);
    assert(node);
    TempTable* output_table = dynamic_cast<TempTable*>(node->getOutputTable());
    assert(output_table);
    Table* input_table = node->getInputTable();
    assert(input_table);

    //
    // OPTIMIZATION: NESTED LIMIT
    // How nice! We can also cut off our scanning with a nested limit!
    //
    int limit = -1;
    int offset = -1;
    if (limit_node != NULL)
    {
        limit_node->getLimitAndOffsetByReference(params, limit, offset);
    }

    VOLT_TRACE("Running OrderBy '%s'", m_abstractNode->debug().c_str());
    VOLT_TRACE("Input Table:\n '%s'", input_table->debug().c_str());
    TableIterator iterator = input_table->iterator();
    TableTuple tuple(input_table->schema());

    // If limit == 0 we have no work here.  There's no need to sort anything,
    // or to fetch the vector of tuples from the input.  If limit < 0 we
    // need to do the loop below, though.  The only case where we can skip
    // is if limit == 0.
    if (limit != 0) {
        vector<TableTuple> xs;
        ProgressMonitorProxy pmp(m_engine->getExecutorContext(), this);
        while (iterator.next(tuple))
        {
            pmp.countdownProgress();
            assert(tuple.isActive());
            xs.push_back(tuple);
        }
        VOLT_TRACE("\n***** Input Table PreSort:\n '%s'",
                   input_table->debug().c_str());


        if (limit >= 0 && xs.begin() + limit + offset < xs.end()) {
            // partial sort
            partial_sort(xs.begin(), xs.begin() + limit + offset, xs.end(),
                    AbstractExecutor::TupleComparer(node->getSortExpressions(), node->getSortDirections()));
        } else {
            // full sort
            sort(xs.begin(), xs.end(),
                    AbstractExecutor::TupleComparer(node->getSortExpressions(), node->getSortDirections()));
        }

        int tuple_ctr = 0;
        int tuple_skipped = 0;
        // If (limit < 0), so we don't have a limit at all, then just compare
        // the iterator with the end.  Otherwise check that the tuple_counter is
        // not over the limit.
        for (vector<TableTuple>::iterator it = xs.begin();
             ((limit < 0) || (tuple_ctr < limit)) && it != xs.end();
             it++)
        {
            //
            // Check if has gone past the offset
            //
            if (tuple_skipped < offset) {
                tuple_skipped++;
                continue;
            }

            VOLT_TRACE("\n***** Input Table PostSort:\n '%s'",
                       input_table->debug().c_str());
            output_table->insertTempTuple(*it);
            pmp.countdownProgress();
            tuple_ctr += 1;
        }
    }
    VOLT_TRACE("Result of OrderBy:\n '%s'", output_table->debug().c_str());

    cleanupInputTempTable(input_table);

    return true;
}

OrderByExecutor::~OrderByExecutor() {
}
