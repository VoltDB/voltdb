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

#ifndef HSTORERANKSCANNODE_H
#define HSTORERANKSCANNODE_H

#include "abstractscannode.h"

namespace voltdb {

class RankExpression;

/**
 *
 */
class RankScanPlanNode : public AbstractScanPlanNode {
public:
    RankScanPlanNode()
        : m_lookupType(INDEX_LOOKUP_TYPE_EQ), m_endType(INDEX_LOOKUP_TYPE_EQ)
    { }
    ~RankScanPlanNode();
    PlanNodeType getPlanNodeType() const;
    std::string debugInfo(const std::string &spacer) const;

    IndexLookupType getLookupType() const { return m_lookupType; }
    IndexLookupType getEndType() const { return m_endType; }

    RankExpression* getRankExpression() const;

    AbstractExpression* getRankKeyExpression() const { return m_rankkey_expression.get(); }

    AbstractExpression* getEndExpression() const { return m_end_expression.get(); }

protected:
    void loadFromJSONObject(PlannerDomValue obj);
    // Index Lookup Type
    IndexLookupType m_lookupType;
    IndexLookupType m_endType;

    // TODO: Document
    boost::scoped_ptr<AbstractExpression> m_rank_expression;

    // TODO: Document
    boost::scoped_ptr<AbstractExpression> m_rankkey_expression;

    // TODO: Document
    boost::scoped_ptr<AbstractExpression> m_end_expression;
};

} // namespace voltdb

#endif
