/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

#include <sstream>

#include "subqueryexpression.h"

#include "common/debuglog.h"
#include "common/executorcontext.hpp"
#include "common/NValue.hpp"
#include "common/tabletuple.h"
#include "executors/executorutil.h"
#include "storage/table.h"
#include "storage/tableiterator.h"


namespace voltdb {

SubqueryExpression::SubqueryExpression(
        ExpressionType subqueryType,
        int subqueryId,
        std::vector<int> paramIdxs,
        std::vector<int> otherParamIdxs,
        const std::vector<AbstractExpression*>* tveParams) :
            AbstractExpression(subqueryType),
            m_subqueryId(subqueryId),
            m_paramIdxs(paramIdxs),
            m_otherParamIdxs(otherParamIdxs),
            m_tveParams(tveParams),
            m_parameterContainer(NULL)
{
    VOLT_TRACE("SubqueryExpression %d", subqueryId);
    m_parameterContainer = ExecutorContext::getExecutorContext()->getParameterContainer();
    assert((m_tveParams.get() == NULL && m_paramIdxs.empty()) ||
        (m_tveParams.get() != NULL && m_paramIdxs.size() == m_tveParams->size()));
    assert(subqueryType == EXPRESSION_TYPE_IN_SUBQUERY || subqueryType == EXPRESSION_TYPE_EXISTS_SUBQUERY ||
            subqueryType == EXPRESSION_TYPE_SCALAR_SUBQUERY || subqueryType == EXPRESSION_TYPE_ROW_SUBQUERY);
}

SubqueryExpression::~SubqueryExpression()
{
    if (m_tveParams.get() != NULL) {
        size_t i = m_tveParams->size();
        while (i--) {
            delete (*m_tveParams)[i];
        }
    }
}

NValue SubqueryExpression::eval(const TableTuple *tuple1, const TableTuple *tuple2) const
{
    // Get the subquery context with the last evaluation result and parameters used to obtain that result

    ExecutorContext* exeContext = ExecutorContext::getExecutorContext();

    SubqueryContext* context = exeContext->getSubqueryContext(m_subqueryId);

    bool hasPriorResult = context != NULL;
    bool paramsChanged = !hasPriorResult;
    VOLT_TRACE ("Running subquery: %d", m_subqueryId);

    // Substitute parameters.
    if (m_tveParams.get() != NULL) {
        size_t paramsCnt = m_tveParams->size();
        for (size_t i = 0; i < paramsCnt; ++i) {
            AbstractExpression* tveParam = (*m_tveParams)[i];
            NValue param = tveParam->eval(tuple1, tuple2);
            // compare the new param value with the previous one. Since this parameter is set
            // by this subquery, no other subquery can change it value. So, we don't need to
            // save its value on a side for future comparisons.
            NValue& prevParam = (*m_parameterContainer)[m_paramIdxs[i]];
            if (hasPriorResult) {
                if (param.compare(prevParam) != VALUE_COMPARE_EQUAL) {
                    prevParam = NValue::copyNValue(param);
                    paramsChanged = true;
                }
            } else {
                prevParam = NValue::copyNValue(param);
            }
        }
    }

    // Compare the other parameter values since the last invocation.
    if (hasPriorResult) {
        std::vector<NValue>& lastParams = context->getLastParams();
        assert(lastParams.size() == m_otherParamIdxs.size());
        for (size_t i = 0; i < lastParams.size(); ++i) {
            NValue& prevParam = (*m_parameterContainer)[m_otherParamIdxs[i]];
            bool paramChanged = lastParams[i].compare(prevParam) != VALUE_COMPARE_EQUAL;
            if (paramChanged) {
                lastParams[i] = NValue::copyNValue(prevParam);
                paramsChanged = true;
            }
        }
    }

    // if parameters haven't changed since the last execution try to reuse the result
    if (!paramsChanged && hasPriorResult) {
        return context->getResult();
    }
    // Out of luck. Need to run the executors
    std::vector<AbstractExecutor*>* executionStack = &exeContext->getExecutorList(m_subqueryId);
    assert(executionStack != NULL);
    assert(!executionStack->empty());

    int status = executeExecutionVector(*executionStack, *m_parameterContainer);

    if (status != ENGINE_ERRORCODE_SUCCESS) {
        char message[256];
        snprintf(message, 256, "Failed to execute the subquery '%d'", m_subqueryId);
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, message);
    }

    NValue retval = NValue::getFalse();

    // The outer_expr IN (SELECT inner_expr ...) evaluates as follows:
    // There is an exact match outer_expr = inner_expr => TRUE
    // There no match and the subquery produces a row where inner_expr is NULL => NULL
    // There no match and the subquery produces only non- NULL rows or empty => FASLE
    // The outer_expr is NULL and the subquery is empty => FASLE
    // The outer_expr is NULL and the subquery produces any row => NULL

    // The EXISTS (SELECT inner_expr ...) evaluates as follows:
    // The subquery produces a row => TRUE
    // The subquery produces an empty result set => FALSE
    Table* outputTable = executionStack->back()->getPlanNode()->getOutputTable();
    assert(outputTable != NULL);
    // Check the first tuple if it's NULL tuple or not
    TableIterator& it = outputTable->iterator();
    TableTuple tuple(outputTable->schema());
    if (it.next(tuple)) {
        if (m_type == EXPRESSION_TYPE_IN_SUBQUERY) {
            // This subquery is part of the IN expression, its top plan node will be a
            // special purpose seqscan node which inserts a tuple with all columns set to NULL
            // to indicate NULL result, so we can try any (the first one) index.
            if (tuple.isNull(0)) {
                retval.setNull();
            } else {
                retval = NValue::getTrue();
            }
        } else if (m_type == EXPRESSION_TYPE_EXISTS_SUBQUERY) {
            // This subquery is part of the EXISTS expression, the EXISTS expression
            // only cares whether the tuple exists or not.
            retval = NValue::getTrue();
        } else if (m_type == EXPRESSION_TYPE_SCALAR_SUBQUERY) {
            // This subquery is part of the SCALAR subquery expression. The scalar subquery is guaranteed
            // to have a single output column and must have no more than a single returned row.
            if (outputTable->activeTupleCount() == 1) {
                retval = tuple.getNValue(0);
            } else if (outputTable->activeTupleCount() > 1) {
                // throw runtime exception
                char message[256];
                snprintf(message, 256, "More than one row returned by a subquery used as an expression");
                throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, message);
            }
        } else {
            // unsupported subquery
            char message[256];
            snprintf(message, 256, "Unsupported expression type: %d", m_type);
            throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, message);
        }
    } else if (m_type == EXPRESSION_TYPE_SCALAR_SUBQUERY) {
        const TupleSchema* tableSchema = outputTable->schema();
        assert(tableSchema != NULL);
        ValueType columnType = tableSchema->columnType(0);
        retval = NValue::getNullValue(columnType);
    }

    if (hasPriorResult) {
        // simply update the result. All params are already updated
        context->setResult(retval);
    } else {
        // Preserve the value for the next run. Only 'other' parameters need to be copied
        std::vector<NValue> lastParams;
        lastParams.reserve(m_otherParamIdxs.size());
        for (size_t i = 0; i < m_otherParamIdxs.size(); ++i) {
            lastParams.push_back(NValue::copyNValue((*m_parameterContainer)[m_otherParamIdxs[i]]));
        }
        SubqueryContext newContext(m_subqueryId, retval, lastParams);
        ExecutorContext::getExecutorContext()->setSubqueryContext(m_subqueryId, newContext);
    }

    exeContext->getEngine()->cleanupExecutorList(*executionStack);

    return retval;
}

std::string SubqueryExpression::debugInfo(const std::string &spacer) const {
    std::ostringstream buffer;
    buffer << spacer << "SubqueryExpression: " << m_subqueryId;
    return (buffer.str());
}

}
