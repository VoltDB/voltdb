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


#ifndef VOLTDB_PLANNODEFRAGMENT_HPP
#define VOLTDB_PLANNODEFRAGMENT_HPP

#include <vector>
#include <map>
#include <list>

#include "common/PlannerDomValue.h"
#include "common/common.h"
#include "common/serializeio.h"
#include "catalog/catalog.h"
#include "catalog/database.h"

namespace voltdb {

class AbstractPlanNode;


/**
 * Represents the full intra-fragment node relationships and provides
 * a viable (but not necessarily exclusive) node execution order.
 */
class PlanNodeFragment {

  public:
    PlanNodeFragment();
    virtual ~PlanNodeFragment();

    // construct a new fragment from the catalog's serialization
    static PlanNodeFragment * createFromCatalog(const std::string);

    // construct a new fragment from a serialized json object
    static PlanNodeFragment* fromJSONObject(PlannerDomValue obj);

    // construct a new fragment from a root node (used by testcode)
    PlanNodeFragment(AbstractPlanNode *root_node);
    void constructTree(AbstractPlanNode *node);

    // first node in serialization order
    AbstractPlanNode * getRootNode() {
        return m_planNodes.front();
    }

    // the list of plannodes in execution order
    inline const std::vector<AbstractPlanNode*>& getExecuteList() const {
        return m_executionList;
    }

    // true if this plan fragment contains a delete plan node.  Used
    // as part of the horrible ENG-1333 hack.
    bool hasDelete() const;

    // produce a string describing pnf's content
    std::string debug();

    // Get the list of parameters used to execute this plan fragment
    std::vector<std::pair< int, voltdb::ValueType> > getParameters() { return parameters; }

  private:

    // reads execute list from plannodelist json objects
    void loadFromJSONObject(PlannerDomValue obj);

    // serialized java type: org.voltdb.plannodes.PlanNode[List|Tree]
    std::string m_serializedType;
    // translate id from catalog to pointer to plannode
    std::map<CatalogId, AbstractPlanNode*> m_idToNodeMap;
    // pointers to nodes in execution order
    std::vector<AbstractPlanNode*> m_executionList;
    // pointers to nodes in serialization order
    std::vector<AbstractPlanNode*> m_planNodes;
    // Pairs of argument index and type for parameters to the fragment
    std::vector<std::pair< int, voltdb::ValueType> > parameters;
};


}



#endif
