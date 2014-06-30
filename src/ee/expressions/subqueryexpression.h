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

#include <boost/shared_ptr.hpp>

#include "common/valuevector.h"
#include "expressions/abstractexpression.h"

namespace voltdb {

class NValue;

class SubqueryExpression : public AbstractExpression {
    public:
    SubqueryExpression(int subqueryId,
        std::vector<int> paramIdxs,
        std::vector<int> otherParamIdxs,
        const std::vector<AbstractExpression*>* tveParams);

    ~SubqueryExpression();

    NValue eval(const TableTuple *tuple1, const TableTuple *tuple2) const;

    void setParentExpressionType(ExpressionType parentType);

    std::string debugInfo(const std::string &spacer) const;

  private:
    int m_subqueryId;
    // The list of parameter indexes that need to be set by this subquery
    // before the expression can be evaluated
    std::vector<int> m_paramIdxs;
    // The list of parameter indexes that this subquery depends upon including
    // its child subqueries but not set by this subquery. Theu originate at
    // the grandparent levels.
    std::vector<int> m_otherParamIdxs;
    // The list of the corresponding TVE for each parameter index
    boost::shared_ptr<const std::vector<AbstractExpression*> > m_tveParams;
    // The pointer to the global parameters
    NValueArray* m_parameterContainer;
    // The parent expression type - either EXISTS or IN
    ExpressionType m_parentType;

};

inline void SubqueryExpression::setParentExpressionType(ExpressionType parentType) {
    m_parentType = parentType;
}
}
#endif
