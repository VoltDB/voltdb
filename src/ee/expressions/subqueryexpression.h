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

#ifndef HSTORESUBQUERYEXPRESSION_H
#define HSTORESUBQUERYEXPRESSION_H

#include <vector>
#include <sstream>

#include "expressions/abstractexpression.h"
#include "common/NValue.hpp"

namespace voltdb {

class SubqueryExpression : public AbstractExpression {
    public:
    SubqueryExpression(int subqueryId);

    voltdb::NValue
    eval(const TableTuple *tuple1, const TableTuple *tuple2) const
    {
        VOLT_TRACE ("Running subquery: %d", m_subqueryId);
        assert(false);
        return voltdb::NValue();
    }

    std::string debugInfo(const std::string &spacer) const {
        std::ostringstream buffer;
        buffer << spacer << "SubqueryExpression: " << m_subqueryId;
        return (buffer.str());
    }

  protected:
    int m_subqueryId;
//    pointers to subqueries nodes in execution order
//    std::vector<AbstractPlanNode*>* m_subqueryExecutionList;
//    boost::shared_ptr<ExecutorVector> m_executorVector;
};
}
#endif
