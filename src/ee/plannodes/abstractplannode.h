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

#include "SchemaColumn.h"

#include "catalog/database.h"
#include "common/ids.h"
#include "common/debuglog.h"
#include "common/types.h"
#include "common/PlannerDomValue.h"

#include <map>
#include <string>
#include <vector>

#include "boost/scoped_ptr.hpp"

namespace voltdb {

class AbstractExecutor;
class AbstractExpression;
class Table;
class TableCatalogDelegate;
class AbstractTempTable;
class TupleSchema;

class AbstractPlanNode {
public:
    virtual ~AbstractPlanNode();

    // ------------------------------------------------------------------
    // CHILDREN + PARENTS METHODS
    // ------------------------------------------------------------------
    void addChild(AbstractPlanNode* child) { m_children.push_back(child); }
    const std::vector<int32_t>& getChildIds() const { return m_childIds; }
    const std::vector<AbstractPlanNode*>& getChildren() const { return m_children; }

    // ------------------------------------------------------------------
    // INLINE PLANNODE METHODS
    // ------------------------------------------------------------------
    void addInlinePlanNode(AbstractPlanNode* inline_node);
    AbstractPlanNode* getInlinePlanNode(PlanNodeType type) const;
    const std::map<PlanNodeType, AbstractPlanNode*>& getInlinePlanNodes() const { return m_inlineNodes; }
    bool isInline() const { return m_isInline; }

    // ------------------------------------------------------------------
    // DATA MEMBER METHODS
    // ------------------------------------------------------------------
    int32_t getPlanNodeId() const { return m_planNodeId; }

    // currently a hack needed to initialize the executors.
    CatalogId databaseId() const { return 1; }

    void setExecutor(AbstractExecutor* executor);
    AbstractExecutor* getExecutor() const { return m_executor.get(); }

    class TableReference {
    public:
        TableReference() : m_tcd(NULL), m_tempTable(NULL) { }

        Table* getTable() const;

        AbstractTempTable* getTempTable() const {
            return m_tempTable;
        }

        void setTable(TableCatalogDelegate* tcd)
        {
            vassert(! m_tcd);
            vassert(! m_tempTable);
            m_tcd = tcd;
        }

        void setTable(AbstractTempTable* table)
        {
            vassert(! m_tcd);
            vassert(! m_tempTable);
            m_tempTable = table;
        }

        void clearTable()
        {
            m_tcd = NULL;
            m_tempTable = NULL;
        }

    private:

        TableCatalogDelegate* m_tcd;
        AbstractTempTable* m_tempTable;
    };

    // Adds cleanup behavior that only effects output temp tables.
    class TableOwner : public TableReference {
    public:
        ~TableOwner();
    };

    void setInputTables(const std::vector<Table*> &val);
    size_t getInputTableCount() const { return m_inputTables.size(); }
    const std::vector<TableReference>& getInputTableRefs() { return m_inputTables; }

    Table *getInputTable() const { return m_inputTables[0].getTable(); }

    Table *getInputTable(int which) const { return m_inputTables[which].getTable(); }

    AbstractTempTable *getTempInputTable() const { return m_inputTables[0].getTempTable(); }

    void setOutputTable(Table* val);
    void clearOutputTableReference() { m_outputTable.clearTable(); }
    Table *getOutputTable() const { return m_outputTable.getTable(); }
    AbstractTempTable *getTempOutputTable() const { return m_outputTable.getTempTable(); }

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
     * Get the output number of columns -- strictly for use with plannode
     * classes that "project" a new output schema (vs. passing one up from a child).
     * This is cleaner than using "getOutputSchema().size()" in such cases, such as Projection nodes,
     * when m_outputSchema and m_validOutputColumnCount are known to be valid and in agreement.
     */
    int getValidOutputColumnCount() const
    {
        // Assert that this plan node defined (derialized in) its own output schema.
        vassert(m_validOutputColumnCount >= 0);
        return m_validOutputColumnCount;
    }

    /**
     * Convenience method:
     * Generate a TupleSchema based on the contents of the output schema
     * from the plan
     */
    TupleSchema* generateTupleSchema() const;

    /**
     * Convenience method:
     * Generate a TupleSchema based on the expected format for DML results.
     */
    static TupleSchema* generateDMLCountTupleSchema();

    // ------------------------------------------------------------------
    // UTILITY METHODS
    // ------------------------------------------------------------------
    static std::unique_ptr<AbstractPlanNode> fromJSONObject(PlannerDomValue obj);

    // Debugging convenience methods
    std::string debug() const;
    std::string debug(const std::string& spacer) const;
    virtual std::string debugInfo(const std::string& spacer) const = 0;

    void setPlanNodeIdForTest(int32_t plannode_id) { m_planNodeId = plannode_id; }

    /**
     * Load list of sort expressions and directions from a JSON object.
     * The pointers may be null if one of the vectors is not wanted.
     */
    static void loadSortListFromJSONObject(PlannerDomValue obj,
                                           std::vector<AbstractExpression*> *sortExprs,
                                           std::vector<SortDirectionType>   *sortDirs);

    // A simple method of managing the lifetime of AbstractExpressions referenced by
    // a vector that is never mutated once it is loaded.
    struct OwningExpressionVector : public std::vector<AbstractExpression*> {
        // Nothing prevents the vector from being set up or even modified
        // via other vector methods, with this caveat:
        // The memory management magic provided here simply assumes ownership
        // of any elements referenced by the _final_ state of the vector.
        ~OwningExpressionVector();
        void loadExpressionArrayFromJSONObject(const char* label,
                                               PlannerDomValue obj);
    };

protected:
    AbstractPlanNode();

    virtual void loadFromJSONObject(PlannerDomValue obj) = 0;

    // Common code for use by the public generateTupleSchema() overload
    // and by AbstractJoinPlanNode::loadFromJSONObject for its pre-agg output tuple.
    static TupleSchema* generateTupleSchema(const std::vector<SchemaColumn*>& outputSchema);

    static void loadIntArrayFromJSONObject(const char* label,
                                           PlannerDomValue obj,
                                           std::vector<int>& ary);

    static void loadStringArrayFromJSONObject(const char* label,
                                              PlannerDomValue obj,
                                              std::vector<std::string>& ary);

    static void loadBooleanArrayFromJSONObject(const char* label,
                                              PlannerDomValue obj,
                                              std::vector<bool>& ary);

    static AbstractExpression* loadExpressionFromJSONObject(const char* label,
                                                            PlannerDomValue obj);

    // Every PlanNode will have a unique id assigned to it at compile time
    int32_t m_planNodeId = -1;

    //
    // A node can have multiple children references, initially serialized as Ids
    //
    std::vector<AbstractPlanNode*> m_children{};
    std::vector<int32_t> m_childIds{};

    // Keep a pointer to this node's executor for memeory management purposes.
    boost::scoped_ptr<AbstractExecutor> m_executor;

    // Some Executors can take advantage of multiple internal PlanNodes to perform tasks inline.
    // This can be a big speed increase and/or temp table memory decrease.
    std::map<PlanNodeType, AbstractPlanNode*> m_inlineNodes{};
    // This PlanNode may be getting referenced in that way.
    // Currently, it is still assigned an executor that either goes unused or
    // provides some service to the parent executor. This allows code sharing between inline
    // and non-inline uses of the same PlanNode type.
    bool m_isInline = false;

private:
    static const int SCHEMA_UNDEFINED_SO_GET_FROM_INLINE_PROJECTION = -1;
    static const int SCHEMA_UNDEFINED_SO_GET_FROM_CHILD = -2;

    // Output Table
    // This is where we will write the results of the plan node's execution
    TableOwner m_outputTable{};

    // Input Tables
    // These tables are derived from the output of this node's children
    std::vector<TableReference> m_inputTables{};

    // This is mostly used to hold one of the SCHEMA_UNDEFINED_SO_GET_FROM_ flags
    // or some/any non-negative value indicating that m_outputSchema is valid.
    // the fact that it also matches the size of m_outputSchema -- when it is valid
    // -- MIGHT come in handy?
    int m_validOutputColumnCount = 0;
    std::vector<SchemaColumn*> m_outputSchema{};
};

} // namespace voltdb

