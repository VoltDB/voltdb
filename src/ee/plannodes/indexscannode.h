/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
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

#pragma once

#include "abstractscannode.h"

namespace voltdb {

/**
 *
 */
class IndexScanPlanNode : public AbstractScanPlanNode {
public:
    IndexScanPlanNode() = default;
    ~IndexScanPlanNode();
    PlanNodeType getPlanNodeType() const;
    std::string debugInfo(const std::string &spacer) const;

    IndexLookupType getLookupType() const {
        return m_lookup_type;
    }

    bool hasOffsetRankOptimization() const {
        return m_hasOffsetRank;
    }

    SortDirectionType getSortDirection() const {
        return m_sort_direction;
    }

    std::string getTargetIndexName() const {
        return m_target_index_name;
    }

    const std::vector<AbstractExpression*>& getSearchKeyExpressions() const {
        return m_searchkey_expressions;
    }

    const std::vector<bool>& getCompareNotDistinctFlags() const {
        return m_compare_not_distinct;
    }

    AbstractExpression* getEndExpression() const {
        return m_end_expression.get();
    }

    AbstractExpression* getInitialExpression() const {
        return m_initial_expression.get();
    }

    AbstractExpression* getSkipNullPredicate() const {
        return m_skip_null_predicate.get();
    }
protected:
    void loadFromJSONObject(PlannerDomValue obj);

    // This is the id of the index to reference during execution
    std::string m_target_index_name;

    // TODO: Document
    OwningExpressionVector m_searchkey_expressions{};

    // If the search key expression is actually a "not distinct" expression,
    //   we do not want the executor to skip null candidates.
    // This flag vector will instruct the executor the correct behavior for null skipping. (ENG-11096)
    std::vector<bool> m_compare_not_distinct{};

    // TODO: Document
    boost::scoped_ptr<AbstractExpression> m_end_expression{};

    // TODO: Document
    boost::scoped_ptr<AbstractExpression> m_initial_expression{};

    // Index Lookup Type
    IndexLookupType m_lookup_type = IndexLookupType::Equal;

    // Offset rank
    bool m_hasOffsetRank = false;

    // Sorting Direction
    SortDirectionType m_sort_direction = SORT_DIRECTION_TYPE_INVALID;

    // null row predicate for underflow edge case
    boost::scoped_ptr<AbstractExpression> m_skip_null_predicate{};
};

} // namespace voltdb

