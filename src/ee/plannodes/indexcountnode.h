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


#pragma once

#include "abstractscannode.h"

namespace voltdb {

/**
 *
 */
class IndexCountPlanNode : public AbstractScanPlanNode {
public:
    IndexCountPlanNode() = default;

    ~IndexCountPlanNode();
    PlanNodeType getPlanNodeType() const;
    std::string debugInfo(const std::string &spacer) const;

    IndexLookupType getLookupType() const {
        return m_lookup_type;
    }

    IndexLookupType getEndType() const {
        return m_end_type;
    }

    const std::string& getTargetIndexName() const {
        return m_target_index_name;
    }

    const std::vector<AbstractExpression*>& getEndKeyExpressions() const {
        return m_endkey_expressions;
    }

    const std::vector<AbstractExpression*>& getSearchKeyExpressions() const {
        return m_searchkey_expressions;
    }

    const std::vector<bool>& getCompareNotDistinctFlags() const {
        return m_compare_not_distinct;
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
    OwningExpressionVector m_endkey_expressions{};

    // Index Lookup Type
    IndexLookupType m_lookup_type = IndexLookupType::Equal;

    // Index Lookup End Type
    IndexLookupType m_end_type = IndexLookupType::Equal;

    // count null row predicate for edge cases: reverse scan or underflow case
    boost::scoped_ptr<AbstractExpression> m_skip_null_predicate{};
};

} // namespace voltdb

