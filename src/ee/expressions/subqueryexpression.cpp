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
#include <iostream>

#include "subqueryexpression.h"

#include "common/debuglog.h"
#include "common/executorcontext.hpp"
#include "common/NValue.hpp"
#include "executors/executorutil.h"

namespace voltdb {

    SubqueryExpression::SubqueryExpression(int subqueryId,
        const std::vector<int>& paramIdxs,
        const std::vector<int>& allParamIdxs,
        const std::vector<AbstractExpression*>* tveParams) :
            AbstractExpression(EXPRESSION_TYPE_SUBQUERY),
            m_subqueryId(subqueryId),
            m_paramIdxs(paramIdxs),
            m_allParamIdxs(allParamIdxs),
            m_tveParams(tveParams),
            m_parameterContainer(NULL)
    {
        VOLT_TRACE("SubqueryExpression %d", subqueryId);
        m_parameterContainer = &ExecutorContext::getExecutorContext()->getParameterContainer();
        assert((m_tveParams.get() == NULL && m_paramIdxs.empty()) ||
            (m_tveParams.get() != NULL && m_paramIdxs.size() == m_tveParams->size()));
    }

    SubqueryExpression::~SubqueryExpression() {
        if (m_tveParams.get() != NULL) {
            size_t i = m_tveParams->size();
            while (i--) {
                delete (*m_tveParams)[i];
            }
        }
    }

    NValue
    SubqueryExpression::eval(const TableTuple *tuple1, const TableTuple *tuple2) const
    {
std::cout << "========== Running subquery:   " << m_subqueryId << '\n';
        VOLT_TRACE ("Running subquery: %d", m_subqueryId);
        // Substitute parameters.
        if (m_tveParams.get() != NULL) {
            size_t paramsCnt = m_tveParams->size();
            for (size_t i = 0; i < paramsCnt; ++i) {
                AbstractExpression* tveParam = (*m_tveParams)[i];
                NValue param = tveParam->eval(tuple1, tuple2);
                // compare the new param value with the previous one
                // ENG-451 - how to compare NValues (possible NULLS)
                // preserver the new value
                // ENG-451 do we need a special copy there? or default copy is enough?
                (*m_parameterContainer)[m_paramIdxs[i]] = param;
            }
        }

        // Keep track whether any of them have changed
        // since the last invocation. If not, the previous invocation result (if available)
        // can be re-used.
        SubqueryContext* context = ExecutorContext::getExecutorContext()->getSubqueryContext(m_subqueryId);
        bool paramsChanged = false;
        bool hasPriorResult = context != NULL;
std::cout << "==========Running subquery:   " << m_subqueryId << ", has results= " << hasPriorResult << '\n';

        if (hasPriorResult) {
            std::vector<NValue>& lastParams = context->getLastParams();
            assert(lastParams.size() == m_allParamIdxs.size());
            for (size_t i = 0; i < lastParams.size(); ++i) {
                bool paramChanged = lastParams[i].compare((*m_parameterContainer)[m_allParamIdxs[i]]) != 0;
std::cout << "Param idx " << m_allParamIdxs[i] << ", changed= " << paramChanged << '\n';
if (paramChanged) {
std::cout << "Old val: " << lastParams[i].debug() << '\n';
std::cout << "New val: " << (*m_parameterContainer)[m_allParamIdxs[i]].debug() << '\n';
}
                if (paramChanged) {
                    // ENG-451 do we need a special copy there? or default copy is enough?
                    lastParams[i] = (*m_parameterContainer)[m_allParamIdxs[i]];
                    paramsChanged = true;
                }
            }
        }


std::cout << "All paramsChanged=" << paramsChanged << '\n';
        // if parameters haven't changed since the last execution try to reuse the result
        if (!paramsChanged && hasPriorResult) {
std::cout << "Using old results\n";
            return context->getResult();
        }
std::cout << "Need to calculate results\n";
        // Out of luck. Need to run the executors
        std::vector<AbstractExecutor*>* executionStack =
            &ExecutorContext::getExecutorContext()->getExecutorList(m_subqueryId);
        assert(executionStack != NULL);
        assert(!executionStack->empty());
        int status = executeExecutionVector(*executionStack, *m_parameterContainer);
        if (status != ENGINE_ERRORCODE_SUCCESS) {
            char message[256];
            snprintf(message, 256, "Failed to execute the subquery '%d'", m_subqueryId);
            throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, message);
        }
        bool result = executionStack->back()->getPlanNode()->getOutputTable()->activeTupleCount() != 0;
        NValue retval = (result) ? NValue::getTrue() : NValue::getFalse();
        // Preserve the value for the next run getSubqueryResult
        std::vector<NValue> lastParams;
        lastParams.reserve(m_allParamIdxs.size());
        for (size_t i = 0; i < m_allParamIdxs.size(); ++i) {
            lastParams.push_back((*m_parameterContainer)[m_allParamIdxs[i]]);
        }
        SubqueryContext newContext(m_subqueryId, retval, lastParams);
        ExecutorContext::getExecutorContext()->setSubqueryContext(m_subqueryId, newContext);
std::cout << "==========Done Running subquery:   " << m_subqueryId << '\n';
        return retval;
    }

}

