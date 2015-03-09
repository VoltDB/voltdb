/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

#ifndef HSTOREINDEXSCANNODE_H
#define HSTOREINDEXSCANNODE_H

#include "abstractscannode.h"

namespace voltdb {

/**
 *
 */
class IndexScanPlanNode : public AbstractScanPlanNode {
public:
    IndexScanPlanNode()
        : m_lookup_type(INDEX_LOOKUP_TYPE_EQ)
        , m_sort_direction(SORT_DIRECTION_TYPE_INVALID)
    { }
    ~IndexScanPlanNode();
    PlanNodeType getPlanNodeType() const;
    std::string debugInfo(const std::string &spacer) const;

    IndexLookupType getLookupType() const { return m_lookup_type; }

    SortDirectionType getSortDirection() const { return m_sort_direction; }

    std::string getTargetIndexName() const { return m_target_index_name; }

    const std::vector<AbstractExpression*>& getSearchKeyExpressions() const
    { return m_searchkey_expressions; }

    AbstractExpression* getEndExpression() const { return m_end_expression.get(); }

    AbstractExpression* getInitialExpression() const { return m_initial_expression.get(); }

    AbstractExpression* getSkipNullPredicate() const { return m_skip_null_predicate.get(); }

protected:
    void loadFromJSONObject(PlannerDomValue obj);

    // This is the id of the index to reference during execution
    std::string m_target_index_name;

    // TODO: Document
    OwningExpressionVector m_searchkey_expressions;

    // TODO: Document
    boost::scoped_ptr<AbstractExpression> m_end_expression;

    // TODO: Document
    boost::scoped_ptr<AbstractExpression> m_initial_expression;

    // Index Lookup Type
    IndexLookupType m_lookup_type;

    // Sorting Direction
    SortDirectionType m_sort_direction;

    // null row predicate for underflow edge case
    boost::scoped_ptr<AbstractExpression> m_skip_null_predicate;
};

} // namespace voltdb

#endif
