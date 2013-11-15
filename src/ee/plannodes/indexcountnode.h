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


#ifndef HSTOREINDEXCOUNTNODE_H
#define HSTOREINDEXCOUNTNODE_H

#include <string>
#include "abstractscannode.h"
#include "common/ValueFactory.hpp"

namespace voltdb {

class AbstractExpression;

/**
 *
 */
class IndexCountPlanNode : public AbstractScanPlanNode {
    public:
        IndexCountPlanNode(CatalogId id) : AbstractScanPlanNode(id) {
            this->key_iterate = false;
            this->lookup_type = INDEX_LOOKUP_TYPE_EQ;
            this->end_type = INDEX_LOOKUP_TYPE_EQ;
        }
        IndexCountPlanNode() : AbstractScanPlanNode() {
            this->key_iterate = false;
            this->lookup_type = INDEX_LOOKUP_TYPE_EQ;
            this->end_type = INDEX_LOOKUP_TYPE_EQ;
        }
        ~IndexCountPlanNode();
        virtual PlanNodeType getPlanNodeType() const { return (PLAN_NODE_TYPE_INDEXCOUNT); }

        void setKeyIterate(bool val);
        bool getKeyIterate() const;

        void setLookupType(IndexLookupType val);
        IndexLookupType getLookupType() const;

        void setEndType(IndexLookupType val);
        IndexLookupType getEndType() const;

        void setTargetIndexName(std::string name);
        std::string getTargetIndexName() const;

        void setSearchKeyExpressions(std::vector<AbstractExpression*> &exps);
        std::vector<AbstractExpression*>& getSearchKeyExpressions();
        const std::vector<AbstractExpression*>& getSearchKeyExpressions() const;

        void setEndKeyEndExpressions(std::vector<AbstractExpression*> &exps);
        std::vector<AbstractExpression*>& getEndKeyExpressions();
        const std::vector<AbstractExpression*>& getEndKeyExpressions() const;

        AbstractExpression* getCountNULLExpression() const;

        std::string debugInfo(const std::string &spacer) const;

    protected:
        virtual void loadFromJSONObject(PlannerDomValue obj);
        //
        // This is the id of the index to reference during execution
        //
        std::string target_index_name;

        //
        // TODO: Document
        //
        std::vector<AbstractExpression*> searchkey_expressions;
        //
        // TODO: Document
        //
        std::vector<AbstractExpression*> endkey_expressions;
        //
        // Enable Index Key Iteration
        //
        bool key_iterate;
        //
        // Index Lookup Type
        //
        IndexLookupType lookup_type;
        //
        // Index Lookup End Type
        //
        IndexLookupType end_type;
        //
        // count null row expression for edge cases: reverse scan or underflow case
        //
        AbstractExpression* count_null_expression;
};

}

#endif
