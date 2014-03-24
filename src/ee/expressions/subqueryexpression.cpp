/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

#include "subqueryexpression.h"

#include "common/debuglog.h"
#include "common/executorcontext.hpp"
#include "common/NValue.hpp"
#include "executors/executorutil.h"

namespace voltdb {

    SubqueryExpression::SubqueryExpression(int subqueryId,
        const std::vector<int>& paramIdxs,
        const std::vector<AbstractExpression*>* tveParams) :
            AbstractExpression(EXPRESSION_TYPE_SUBQUERY),
            m_subqueryId(subqueryId), m_paramIdxs(paramIdxs),
            m_tveParams(tveParams), m_tveParamsSize(0),
            m_executionStack(NULL), m_parameterContainer(NULL)
    {
        VOLT_TRACE("SubqueryExpression %d", subqueryId);
        m_executionStack = &ExecutorContext::getExecutorContext()->getExecutorLists(subqueryId);
        assert(m_executionStack != NULL);
        m_parameterContainer = &ExecutorContext::getExecutorContext()->getParameterContainer();
        if (m_tveParams.get() != NULL) {
            m_tveParamsSize = m_tveParams->size();
        }
        assert(m_paramIdxs.size() == m_tveParamsSize);
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
        VOLT_TRACE ("Running subquery: %d", m_subqueryId);
        // Substitute parameters
        if (m_tveParams.get() != NULL) {
            for (int i = 0; i < m_tveParamsSize; ++i) {
                AbstractExpression* tveParam = (*m_tveParams)[i];
                NValue param = tveParam->eval(tuple1, tuple2);
                (*m_parameterContainer)[m_paramIdxs[i]] = param;
            }
        }
        // Run the executors
        int status = executeExecutionVector(*m_executionStack, *m_parameterContainer);
        if (status != ENGINE_ERRORCODE_SUCCESS) {
            char message[256];
            snprintf(message, 256, "Failed to execute the subquery '%d'", m_subqueryId);
            throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, message);
        }
        assert(!m_executionStack->empty());
        bool result = m_executionStack->back()->getPlanNode()->getOutputTable()->activeTupleCount() != 0;
        return (result) ? NValue::getTrue() : NValue::getFalse();
    }

}

