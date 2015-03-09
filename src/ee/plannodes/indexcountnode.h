/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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


#ifndef HSTOREINDEXCOUNTNODE_H
#define HSTOREINDEXCOUNTNODE_H

#include "abstractscannode.h"

namespace voltdb {

/**
 *
 */
class IndexCountPlanNode : public AbstractScanPlanNode {
public:
    IndexCountPlanNode()
        : m_lookup_type(INDEX_LOOKUP_TYPE_EQ)
        , m_end_type(INDEX_LOOKUP_TYPE_EQ)
    { }
    ~IndexCountPlanNode();
    PlanNodeType getPlanNodeType() const;
    std::string debugInfo(const std::string &spacer) const;

    IndexLookupType getLookupType() const { return m_lookup_type; }

    IndexLookupType getEndType() const { return m_end_type; }

    const std::string& getTargetIndexName() const { return m_target_index_name; }

    const std::vector<AbstractExpression*>& getEndKeyExpressions() const
    { return m_endkey_expressions; }

    const std::vector<AbstractExpression*>& getSearchKeyExpressions() const
    { return m_searchkey_expressions; }

    AbstractExpression* getSkipNullPredicate() const { return m_skip_null_predicate.get(); }

protected:
    void loadFromJSONObject(PlannerDomValue obj);

    // This is the id of the index to reference during execution
    std::string m_target_index_name;

    // TODO: Document
    OwningExpressionVector m_searchkey_expressions;

    // TODO: Document
    OwningExpressionVector m_endkey_expressions;

    // Index Lookup Type
    IndexLookupType m_lookup_type;

    // Index Lookup End Type
    IndexLookupType m_end_type;

    // count null row predicate for edge cases: reverse scan or underflow case
    boost::scoped_ptr<AbstractExpression> m_skip_null_predicate;
};

} // namespace voltdb

#endif
