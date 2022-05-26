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

#include <vector>
#include <map>
#include <list>

#include "common/PlannerDomValue.h"
#include "common/common.h"
#include "common/serializeio.h"
#include "catalog/catalog.h"
#include "catalog/database.h"
#include "expressions/abstractexpression.h"

namespace voltdb {

class AbstractPlanNode;

/**
 * Represents the full intra-fragment node relationships and provides
 * a viable (but not necessarily exclusive) node execution order.
 */
class PlanNodeFragment {
    // construct a new fragment from a serialized json object
    static std::unique_ptr<PlanNodeFragment> fromJSONObject(PlannerDomValue const& planNodesArray);
    // read node list for a given sub statement
    void nodeListFromJSONObject(PlannerDomValue const& planNodesList, PlannerDomValue const& executeList, int stmtId);

    // translate id from catalog to pointer to plannode
    std::map<CatalogId, AbstractPlanNode*> m_idToNodeMap;
    // Pointers to nodes in execution order grouped by substatement
    // The statement id is the key. The top statement (parent) always has id = 0
    std::map<int, std::vector<AbstractPlanNode*>> m_stmtExecutionListMap;

    bool m_isLargeQuery;

public:
    using PlanNodeMapIterator = std::map<int, std::vector<AbstractPlanNode*>>::iterator;
    using PlanNodeMapIteratorConst = std::map<int, std::vector<AbstractPlanNode*>>::const_iterator;

    PlanNodeFragment();
    virtual ~PlanNodeFragment();

    // construct a new fragment from the catalog's serialization
    static PlanNodeFragment* createFromCatalog(const char*);

    // construct a new fragment from a root node (used by testcode)
    PlanNodeFragment(AbstractPlanNode *root_node);
    void constructTree(AbstractPlanNode *node);

    // first node from the statement plan
    AbstractPlanNode* getRootNode(int stmtId = 0) {
        vassert(m_stmtExecutionListMap.find(stmtId) != m_stmtExecutionListMap.end());
        return m_stmtExecutionListMap[stmtId].front();
    }

    // the list of plannodes in execution order for a given sub-statement
    PlanNodeMapIteratorConst executeListBegin() const {
        return m_stmtExecutionListMap.begin();
    }
    PlanNodeMapIterator executeListBegin() {
        return m_stmtExecutionListMap.begin();
    }

    PlanNodeMapIteratorConst executeListEnd() const {
        return m_stmtExecutionListMap.end();
    }
    PlanNodeMapIterator executeListEnd() {
        return m_stmtExecutionListMap.end();
    }

    // true if this plan fragment contains a delete plan node.  Used
    // as part of the horrible ENG-1333 hack.
    bool hasDelete() const;

    bool isLargeQuery() const {
        return m_isLargeQuery;
    }

    // produce a string describing pnf's content
    std::string debug();
};


}

