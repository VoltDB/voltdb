/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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


namespace voltdb {

SubqueryExpression::SubqueryExpression(
        ExpressionType subqueryType,
        int subqueryId,
        const std::vector<int>& paramIdxs,
        const std::vector<int>& otherParamIdxs,
        const std::vector<AbstractExpression*>& tveParams) :
            AbstractExpression(subqueryType),
            m_subqueryId(subqueryId),
            m_paramIdxs(paramIdxs),
            m_otherParamIdxs(otherParamIdxs),
            m_tveParams(tveParams)
{
    VOLT_TRACE("SubqueryExpression %d", subqueryId);
    vassert(m_paramIdxs.size() == m_tveParams.size());
}

SubqueryExpression::~SubqueryExpression() {
   for(auto const* expr : m_tveParams) {
      delete expr;
   }
}

NValue SubqueryExpression::eval(const TableTuple *tuple1, const TableTuple *tuple2) const
{
    // Get the subquery context with the last evaluation result and parameters used to obtain that result
    ExecutorContext* exeContext = ExecutorContext::getExecutorContext();

    SubqueryContext* context = exeContext->getSubqueryContext(m_subqueryId);

    bool hasPriorResult = (context != NULL) && context->hasValidResult();
    bool paramsChanged = false;
    NValueArray& parameterContainer = exeContext->getParameterContainer();
    VOLT_TRACE ("Running subquery: %d", m_subqueryId);

    // Substitute parameters.
    size_t paramsCnt = m_tveParams.size();
    for (size_t i = 0; i < paramsCnt; ++i) {
       NValue param = m_tveParams[i]->eval(tuple1, tuple2);
       // compare the new param value with the previous one. Since this parameter is set
       // by this subquery, no other subquery can change its value. So, we don't need to
       // save its value on the side for future comparisons.
       NValue& prevParam = parameterContainer[m_paramIdxs[i]];
       if (hasPriorResult) {
          if (param.compare(prevParam) == VALUE_COMPARE_EQUAL) {
             continue;
          }
          paramsChanged = true;
       }
       // Update the value stored in the executor context's parameter container:
       prevParam = param.copyNValue();
    }

    // Note the other (non-tve) parameter values and check if they've changed since the last invocation.
    if (hasPriorResult) {
        std::vector<NValue>& lastParams = context->accessLastParams();
        vassert(lastParams.size() == m_otherParamIdxs.size());
        for (size_t i = 0; i < lastParams.size(); ++i) {
            const NValue& prevParam = parameterContainer[m_otherParamIdxs[i]];
            if (lastParams[i].compare(prevParam) != VALUE_COMPARE_EQUAL) {
                lastParams[i] = prevParam.copyNValue();
                paramsChanged = true;
            }
        }
        if (paramsChanged) {
            // If parameters have changed since the last execution,
            // the cached result of the prior execution is obsolete.
            // In particular, it should not be mistaken for the correct result for the current
            // parameters in the event that the current execution fails.
            // This subquery context will be restored to validity when its new result is set
            // after execution succeeds.
            context->invalidateResult();
        } else {
            // If the parameters haven't changed since the last execution, reuse the known result.
            return context->getResult();
        }
    }

    // Out of luck. Need to run the executors. Clean up the output tables with cached results
    exeContext->cleanupExecutorsForSubquery(m_subqueryId);
    UniqueTempTableResult result = exeContext->executeExecutors(m_subqueryId);

    // We don't want this temp table to be cleaned up; we want it to
    // persist for use by the consumer, and to cache the result so it
    // can be reused.
    result.release();

    if (context == NULL) {
        // Preserve the value for the next run. Only 'other' parameters need to be copied
        std::vector<NValue> lastParams;
        lastParams.reserve(m_otherParamIdxs.size());
        for (size_t i = 0; i < m_otherParamIdxs.size(); ++i) {
            NValue& prevParam = parameterContainer[m_otherParamIdxs[i]];
            lastParams.push_back(prevParam.copyNValue());
        }
        context = exeContext->setSubqueryContext(m_subqueryId, lastParams);
    }

    // Update the cached result for the current params. All params are already updated
    NValue retval = ValueFactory::getIntegerValue(m_subqueryId);
    context->setResult(retval);
    return retval;
}

std::string SubqueryExpression::debugInfo(const std::string &spacer) const
{
    std::ostringstream buffer;
    buffer << spacer << expressionToString(getExpressionType()) << ": subqueryId: " << m_subqueryId;
    return buffer.str();
}

}
