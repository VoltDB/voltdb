/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef HSTOREINDEXCOUNTNODE_H
#define HSTOREINDEXCOUNTNODE_H

#include <string>
#include "abstractscannode.h"
#include "json_spirit/json_spirit.h"

namespace voltdb {

class AbstractExpression;

/**
 *
 */
class IndexCountPlanNode : public AbstractScanPlanNode {
    public:
        IndexCountPlanNode(CatalogId id) : AbstractScanPlanNode(id) {
            printf("IndexCountPlanNode runs...1");
            this->key_iterate = false;
            this->lookup_type = INDEX_LOOKUP_TYPE_EQ;
            this->end_expression = NULL;
        }
        IndexCountPlanNode() : AbstractScanPlanNode() {
            printf("IndexCountPlanNode runs...");
            this->key_iterate = false;
            this->lookup_type = INDEX_LOOKUP_TYPE_EQ;
            this->end_expression = NULL;
        }
        ~IndexCountPlanNode();
        virtual PlanNodeType getPlanNodeType() const { return (PLAN_NODE_TYPE_INDEXCOUNT); }

        void setKeyIterate(bool val);
        bool getKeyIterate() const;

        void setLookupType(IndexLookupType val);
        IndexLookupType getLookupType() const;

        void setTargetIndexName(std::string name);
        std::string getTargetIndexName() const;

        void setEndExpression(AbstractExpression* val);
        AbstractExpression* getEndExpression() const;

        void setSearchKeyExpressions(std::vector<AbstractExpression*> &exps);
        std::vector<AbstractExpression*>& getSearchKeyExpressions();
        const std::vector<AbstractExpression*>& getSearchKeyExpressions() const;

        std::string debugInfo(const std::string &spacer) const;

    protected:
        virtual void loadFromJSONObject(json_spirit::Object &obj);
        //
        // This is the id of the index to reference during execution
        //
        std::string target_index_name;

        //
        // TODO: Document
        AbstractExpression* end_expression;
        //
        // TODO: Document
        //
        std::vector<AbstractExpression*> searchkey_expressions;
        //
        // Enable Index Key Iteration
        //
        bool key_iterate;
        //
        // Index Lookup Type
        //
        IndexLookupType lookup_type;
};

}

#endif
