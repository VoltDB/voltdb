/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB L.L.C. are licensed under the following
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

#include <string>
#include "abstractscannode.h"
#include "json_spirit/json_spirit.h"

namespace voltdb {

class AbstractExpression;

/**
 *
 */
class IndexScanPlanNode : public AbstractScanPlanNode {
    public:
        IndexScanPlanNode(CatalogId id) : AbstractScanPlanNode(id) {
            this->key_iterate = false;
            this->lookup_type = INDEX_LOOKUP_TYPE_EQ;
            this->sort_direction = SORT_DIRECTION_TYPE_INVALID;
            this->end_expression = NULL;
        }
        IndexScanPlanNode() : AbstractScanPlanNode() {
            this->key_iterate = false;
            this->lookup_type = INDEX_LOOKUP_TYPE_EQ;
            this->sort_direction = SORT_DIRECTION_TYPE_INVALID;
            this->end_expression = NULL;
        }
        ~IndexScanPlanNode();
        virtual PlanNodeType getPlanNodeType() const { return (PLAN_NODE_TYPE_INDEXSCAN); }

        void setKeyIterate(bool val);
        bool getKeyIterate() const;

        void setLookupType(IndexLookupType val);
        IndexLookupType getLookupType() const;

        void setSortDirection(SortDirectionType val);
        SortDirectionType getSortDirection() const;

        void setTargetIndexName(std::string name);
        std::string getTargetIndexName() const;

        void setEndExpression(AbstractExpression* val);
        AbstractExpression* getEndExpression() const;

        void setSearchKeyExpressions(std::vector<AbstractExpression*> &exps);
        std::vector<AbstractExpression*>& getSearchKeyExpressions();
        const std::vector<AbstractExpression*>& getSearchKeyExpressions() const;

        std::string debugInfo(const std::string &spacer) const;

    protected:
        friend AbstractPlanNode* AbstractPlanNode::fromJSONObject(json_spirit::Object &obj, const catalog::Database *catalog_db);
        virtual void loadFromJSONObject(json_spirit::Object &obj, const catalog::Database *catalog_db);
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
        //
        // Sorting Direction
        //
        SortDirectionType sort_direction;
};

}

#endif
