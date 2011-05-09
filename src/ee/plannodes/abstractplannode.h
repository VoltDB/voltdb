/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

#ifndef HSTOREPLANNODE_H
#define HSTOREPLANNODE_H

#include "SchemaColumn.h"

#include "catalog/database.h"
#include "common/ids.h"
#include "common/types.h"

#include "json_spirit/json_spirit.h"

#include <map>
#include <string>
#include <vector>

namespace voltdb
{
class AbstractExecutor;
class Table;
class TupleSchema;

class AbstractPlanNode
{
public:
    virtual ~AbstractPlanNode();

    // ------------------------------------------------------------------
    // CHILDREN + PARENTS METHODS
    // ------------------------------------------------------------------
    void addChild(AbstractPlanNode* child);
    std::vector<AbstractPlanNode*>& getChildren();
    std::vector<int32_t>& getChildIds();
    const std::vector<AbstractPlanNode*>& getChildren() const;

    void addParent(AbstractPlanNode* parent);
    std::vector<AbstractPlanNode*>& getParents();
    std::vector<int32_t>& getParentIds();
    const std::vector<AbstractPlanNode*>& getParents() const;

    // ------------------------------------------------------------------
    // INLINE PLANNODE METHODS
    // ------------------------------------------------------------------
    void addInlinePlanNode(AbstractPlanNode* inline_node);
    AbstractPlanNode* getInlinePlanNode(PlanNodeType type) const;
    std::map<PlanNodeType, AbstractPlanNode*>& getInlinePlanNodes();
    const std::map<PlanNodeType, AbstractPlanNode*>& getInlinePlanNodes() const;
    bool isInline() const;

    // ------------------------------------------------------------------
    // DATA MEMBER METHODS
    // ------------------------------------------------------------------
    int32_t getPlanNodeId() const;

    // currently a hack needed to initialize the executors.
    CatalogId databaseId() const { return 1; }

    void setExecutor(AbstractExecutor* executor);
    inline AbstractExecutor* getExecutor() const { return m_executor; }

    void setInputTables(const std::vector<Table*> &val);
    std::vector<Table*>& getInputTables();

    void setOutputTable(Table* val);
    Table *getOutputTable() const;

    //
    // Each sub-class will have to implement this function to return their type
    // This is better than having to store redundant types in all the objects
    //
    virtual PlanNodeType getPlanNodeType() const = 0;

    /**
     * Get the output columns that make up the output schema for
     * this plan node.  The column order is implicit in their
     * order in this vector.
     */
    const std::vector<SchemaColumn*>& getOutputSchema() const;

    /**
     * Convenience method:
     * Generate a TupleSchema based on the contents of the output schema
     * from the plan
     *
     * @param allowNulls whether or not the generated schema should
     * permit null values in the output columns
     */
    TupleSchema* generateTupleSchema(bool allowNulls);

    // ------------------------------------------------------------------
    // UTILITY METHODS
    // ------------------------------------------------------------------
    static AbstractPlanNode*
    fromJSONObject(json_spirit::Object& obj);

    // Debugging convenience methods
    std::string debug() const;
    std::string debug(bool traverse) const;
    std::string debug(const std::string& spacer) const;
    virtual std::string debugInfo(const std::string& spacer) const = 0;

    //
    // Generate a new PlanNodeID
    // NOTE: Only use in debugging & testing! The catalogs will
    // generate real ids at deployment
    //
    static int32_t getNextPlanNodeId() {
        static int32_t next = 1000;
        return next++;
    }

protected:
    virtual void loadFromJSONObject(json_spirit::Object& obj) = 0;
    AbstractPlanNode(int32_t plannode_id);
    AbstractPlanNode();

    void setPlanNodeId(int32_t plannode_id);

    //
    // Every PlanNode will have a unique id assigned to it at compile time
    //
    int32_t m_planNodeId;
    //
    // Output Table
    // This is where we will write the results of the plan node's
    // execution out to
    //
    Table* m_outputTable; // volatile
    //
    // Input Tables
    // These tables are derived from the output of this node's children
    //
    std::vector<Table*> m_inputTables; // volatile
    //
    // A node can have multiple children and parents
    //
    std::vector<AbstractPlanNode*> m_children;
    std::vector<int32_t> m_childIds;
    std::vector<AbstractPlanNode*> m_parents;
    std::vector<int32_t> m_parentIds;
    //
    // We also keep a pointer to this node's executor so that we can
    // reference it quickly
    // at runtime without having to look-up a map
    //
    AbstractExecutor* m_executor; // volatile
    //
    // Some Executors can take advantage of multiple internal PlanNodes
    // to perform tasks inline. This can be a big speed increase
    //
    std::map<PlanNodeType, AbstractPlanNode*> m_inlineNodes;
    bool m_isInline;

    std::vector<SchemaColumn*> m_outputSchema;
};

}

#endif
