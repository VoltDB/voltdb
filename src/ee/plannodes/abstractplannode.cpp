/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
#include "abstractplannode.h"

#include "common/TupleSchema.h"
#include "executors/abstractexecutor.h"
#include "plannodeutil.h"
#include "storage/persistenttable.h"
#include "storage/TableCatalogDelegate.hpp"

#include <sstream>
#include <vector>

using namespace std;

namespace voltdb {

AbstractPlanNode::AbstractPlanNode()
    : m_planNodeId(-1),
      m_isInline(false),
      m_validOutputColumnCount(0) { }

AbstractPlanNode::~AbstractPlanNode()
{
    map<PlanNodeType, AbstractPlanNode*>::iterator iter;
    for (iter = m_inlineNodes.begin(); iter != m_inlineNodes.end(); iter++) {
        delete (*iter).second;
    }
    for (int i = 0; i < m_outputSchema.size(); i++) {
        delete m_outputSchema[i];
    }
}

// ------------------------------------------------------------------
// INLINE PLANNODE METHODS
// ------------------------------------------------------------------
void AbstractPlanNode::addInlinePlanNode(AbstractPlanNode* inline_node)
{
    m_inlineNodes[inline_node->getPlanNodeType()] = inline_node;
    inline_node->m_isInline = true;
}

AbstractPlanNode* AbstractPlanNode::getInlinePlanNode(PlanNodeType type) const
{
    map<PlanNodeType, AbstractPlanNode*>::const_iterator lookup =
        m_inlineNodes.find(type);
    AbstractPlanNode* ret = NULL;
    if (lookup != m_inlineNodes.end()) {
        ret = lookup->second;
    }
    else {
        VOLT_TRACE("No internal PlanNode with type '%s' is available for '%s'",
                   planNodeToString(type).c_str(),
                   debug().c_str());
    }
    return ret;
}

// ------------------------------------------------------------------
// DATA MEMBER METHODS
// ------------------------------------------------------------------
void AbstractPlanNode::setExecutor(AbstractExecutor* executor) { m_executor.reset(executor); }

void AbstractPlanNode::setInputTables(const vector<Table*>& val)
{
    size_t ii = val.size();
    m_inputTables.resize(ii);
    while (ii--) {
        PersistentTable* persistentTable = dynamic_cast<PersistentTable*>(val[ii]);
        if (persistentTable) {
            VoltDBEngine* engine = ExecutorContext::getEngine();
            assert(engine);
            TableCatalogDelegate* tcd = engine->getTableDelegate(persistentTable->name());
            m_inputTables[ii].setTable(tcd);
        } else {
            TempTable* tempTable = dynamic_cast<TempTable*>(val[ii]);
            assert(tempTable);
            m_inputTables[ii].setTable(tempTable);
        }
    }
}

void AbstractPlanNode::setOutputTable(Table* table)
{
    PersistentTable* persistentTable = dynamic_cast<PersistentTable*>(table);
    if (persistentTable) {
        VoltDBEngine* engine = ExecutorContext::getEngine();
        TableCatalogDelegate* tcd = engine->getTableDelegate(persistentTable->name());
        m_outputTable.setTable(tcd);
    } else {
        TempTable* tempTable = dynamic_cast<TempTable*>(table);
        assert(tempTable);
        m_outputTable.setTable(tempTable);
    }
}

const vector<SchemaColumn*>& AbstractPlanNode::getOutputSchema() const
{
    // Test for a valid output schema defined at this plan node.
    // 1-or-more column output schemas are always valid.
    // 0-column output schemas are not currently supported,
    // but SHOULD be for certain edge cases.
    // So, leave that door open, at least here.
    if (m_validOutputColumnCount >= 0) {
        return m_outputSchema;
    }
    // If m_validOutputColumnCount indicates with its magic (negative) value
    // that this node does not actually define its own output schema,
    // navigate downward to its first child (normal or inline) that does.

    // NOTE: we have the option of caching the result in the local m_outputSchema vector
    // and updating m_validOutputColumnCount but that would involve deep copies or
    // reference counts or some other memory management scheme.
    // On the other hand, pass-through output schemas aren't accessed that often
    // (or at least don't strictly NEED to be).
    // Best practice is probably to access them only in the executor's init method
    // and cache any details pertinent to execute.

    const AbstractPlanNode* parent = this;
    const AbstractPlanNode* schema_definer = NULL;
    while (true) {
        // An inline child projection is an excellent place to find an output schema.
        if (parent->m_validOutputColumnCount == SCHEMA_UNDEFINED_SO_GET_FROM_INLINE_PROJECTION) {
            schema_definer = parent->getInlinePlanNode(PLAN_NODE_TYPE_PROJECTION);
            DEBUG_ASSERT_OR_THROW_OR_CRASH((schema_definer != NULL),
                                           "Incorrect output schema source for plannode:\n" << debug(""));
            DEBUG_ASSERT_OR_THROW_OR_CRASH((schema_definer->m_validOutputColumnCount >= 0),
                                           "Missing output schema for inline projection:\n" << debug(""));
            return schema_definer->m_outputSchema;
        }

        // A child node is another possible output schema source, but may take some digging.
        if (parent->m_validOutputColumnCount == SCHEMA_UNDEFINED_SO_GET_FROM_CHILD) {
            // Joins always define their own output schema,
            // so there should only be one child to check,
            // EXCEPT for unions, which DO follow the convention of using the first child's
            // output schema, anyway.  So, just assert that there is at least one child node to use.
            DEBUG_ASSERT_OR_THROW_OR_CRASH( ! parent->m_children.empty(),
                                           "Incorrect output schema source for plannode:\n" << debug("") );

            schema_definer = parent->m_children[0];

            DEBUG_ASSERT_OR_THROW_OR_CRASH((schema_definer != NULL),
                                           "Incorrect output schema source for plannode:\n" << debug(""));
            if (schema_definer->m_validOutputColumnCount >= 0) {
                return schema_definer->m_outputSchema;
            }

            // The child is no more an output schema definer than its parent, keep searching.
            parent = schema_definer;
            continue;
        }

        // All the expected cases have been eliminated -- that can't be good.
        break;
    }
    throwFatalLogicErrorStreamed("No valid output schema defined for plannode:\n" << debug(""));
}

TupleSchema* AbstractPlanNode::generateTupleSchema() const
{
    // Get the effective output schema.
    // In general, this may require a search.
    const vector<SchemaColumn*>& outputSchema = getOutputSchema();
    return generateTupleSchema(outputSchema);
}

TupleSchema* AbstractPlanNode::generateTupleSchema(const std::vector<SchemaColumn*>& outputSchema)
{
    int schema_size = static_cast<int>(outputSchema.size());
    vector<voltdb::ValueType> columnTypes;
    vector<int32_t> columnSizes;
    vector<bool> columnAllowNull(schema_size, true);
    vector<bool> columnInBytes;

    for (int i = 0; i < schema_size; i++)
    {
        //TODO: SchemaColumn is a sad little class that holds an expression pointer,
        // a column name that only really comes in handy in one quirky special case,
        // (see UpdateExecutor::p_init) and a bunch of other stuff that doesn't get used.
        // Someone should put that class out of our misery.
        SchemaColumn* col = outputSchema[i];
        AbstractExpression * expr = col->getExpression();
        columnTypes.push_back(expr->getValueType());
        columnSizes.push_back(expr->getValueSize());
        columnInBytes.push_back(expr->getInBytes());
    }

    TupleSchema* schema =
        TupleSchema::createTupleSchema(columnTypes, columnSizes,
                                       columnAllowNull, columnInBytes);
    return schema;
}

TupleSchema* AbstractPlanNode::generateDMLCountTupleSchema()
{
    // Assuming the expected output schema here saves the expense of hard-coding it into each DML plan.
    vector<voltdb::ValueType> columnTypes(1, VALUE_TYPE_BIGINT);
    vector<int32_t> columnSizes(1, sizeof(int64_t));
    vector<bool> columnAllowNull(1, false);
    vector<bool> columnInBytes(1, false);
    TupleSchema* schema = TupleSchema::createTupleSchema(columnTypes, columnSizes,
            columnAllowNull, columnInBytes);
    return schema;
}


// ----------------------------------------------------
//  Serialization Functions
// ----------------------------------------------------
AbstractPlanNode* AbstractPlanNode::fromJSONObject(PlannerDomValue obj)
{

    string typeString = obj.valueForKey("PLAN_NODE_TYPE").asStr();

    //FIXME: EVEN if this leak guard is warranted --
    // like we EXPECT to be catching plan deserialization exceptions
    // and our biggest concern will be the memory this may leak? --
    // we don't need to be mediating all the node
    // pointer dereferences through the smart pointer.
    // Why not just get() it and forget it until .release() time?
    // As is, it just makes single-step debugging awkward.
    auto_ptr<AbstractPlanNode> node(
        plannodeutil::getEmptyPlanNode(stringToPlanNode(typeString)));

    node->m_planNodeId = obj.valueForKey("ID").asInt();

    if (obj.hasKey("INLINE_NODES")) {
        PlannerDomValue inlineNodesValue = obj.valueForKey("INLINE_NODES");
        for (int i = 0; i < inlineNodesValue.arrayLen(); i++) {
            PlannerDomValue inlineNodeObj = inlineNodesValue.valueAtIndex(i);
            AbstractPlanNode *newNode = AbstractPlanNode::fromJSONObject(inlineNodeObj);

            // todo: if this throws, new Node can be leaked.
            // As long as newNode is not NULL, this will not throw.
            assert(newNode);
            node->addInlinePlanNode(newNode);
        }
    }

    loadIntArrayFromJSONObject("CHILDREN_IDS", obj, node->m_childIds);

    // Output schema are optional -- when they can be determined by a child's copy.
    if (obj.hasKey("OUTPUT_SCHEMA")) {
        PlannerDomValue outputSchemaArray = obj.valueForKey("OUTPUT_SCHEMA");
        for (int i = 0; i < outputSchemaArray.arrayLen(); i++) {
            PlannerDomValue outputColumnValue = outputSchemaArray.valueAtIndex(i);
            SchemaColumn* outputColumn = new SchemaColumn(outputColumnValue, i);
            node->m_outputSchema.push_back(outputColumn);
        }
        node->m_validOutputColumnCount = static_cast<int>(node->m_outputSchema.size());
    }

    // Anticipate and mark the two different scenarios of missing output schema.
    // The actual output schema can be searched for on demand once the whole plan tree is loaded.
    // If there's an inline projection node,
    // one of its chief purposes is defining the parent's output schema.
    else if (node->getInlinePlanNode(PLAN_NODE_TYPE_PROJECTION)) {
        node->m_validOutputColumnCount = SCHEMA_UNDEFINED_SO_GET_FROM_INLINE_PROJECTION;
    }

    // Otherwise, the node is relying on a child's output schema, possibly several levels down,
    // OR it is just an inline node (e.g. a LIMIT) or a DML node,
    // whose output schema is known from its context or is otherwise not of any interest.
    else {
        node->m_validOutputColumnCount = SCHEMA_UNDEFINED_SO_GET_FROM_CHILD;
    }

    node->loadFromJSONObject(obj);

    AbstractPlanNode* retval = node.get();
    node.release();
    assert(retval);
    return retval;
}

void AbstractPlanNode::loadIntArrayFromJSONObject(const char* label,
        PlannerDomValue obj, std::vector<int>& result)
{
    if (obj.hasNonNullKey(label)) {
        PlannerDomValue intArray = obj.valueForKey(label);
        for (int i = 0; i < intArray.arrayLen(); i++) {
            result.push_back(intArray.valueAtIndex(i).asInt());
        }
    }
}

AbstractExpression* AbstractPlanNode::loadExpressionFromJSONObject(const char* label,
                                                                   PlannerDomValue obj)
{
    if (obj.hasNonNullKey(label)) {
        return AbstractExpression::buildExpressionTree(obj.valueForKey(label));
    }
    return NULL;
}

// ------------------------------------------------------------------
// UTILITY METHODS
// ------------------------------------------------------------------
string AbstractPlanNode::debug() const
{
    ostringstream buffer;
    buffer << planNodeToString(getPlanNodeType())
           << "[" << getPlanNodeId() << "]";
    return buffer.str();
}

string AbstractPlanNode::debug(const string& spacer) const
{
    ostringstream buffer;
    buffer << spacer << "* " << debug() << "\n";
    string info_spacer = spacer + "  |";
    buffer << debugInfo(info_spacer);
    //
    // Inline PlanNodes
    //
    if (!m_inlineNodes.empty()) {
        buffer << info_spacer << "Inline Plannodes: "
               << m_inlineNodes.size() << "\n";
        string internal_spacer = info_spacer + "  ";
        map<PlanNodeType, AbstractPlanNode*>::const_iterator it;
        for (it = m_inlineNodes.begin(); it != m_inlineNodes.end(); it++) {
            buffer << info_spacer << "Inline "
                   << planNodeToString(it->second->getPlanNodeType())
                   << ":\n";
            buffer << it->second->debugInfo(internal_spacer);
        }
    }
    //
    // Traverse the tree
    //
    string child_spacer = spacer + "  ";
    for (int ctr = 0, cnt = static_cast<int>(m_children.size()); ctr < cnt; ctr++) {
        buffer << child_spacer << m_children[ctr]->getPlanNodeType() << "\n";
        buffer << m_children[ctr]->debug(child_spacer);
    }
    return (buffer.str());
}

// AbstractPlanNode nested class methods

Table* AbstractPlanNode::TableReference::getTable() const
{
    if (m_tcd) {
        return m_tcd->getTable();
    }
    return m_tempTable;
}

AbstractPlanNode::TableOwner::~TableOwner() { delete m_tempTable; }

AbstractPlanNode::OwningExpressionVector::~OwningExpressionVector()
{
    size_t each = size();
    while (each--) {
        delete (*this)[each];
    }
}

void AbstractPlanNode::OwningExpressionVector::loadExpressionArrayFromJSONObject(const char* label,
                                                                                 PlannerDomValue obj)
{
    clear();
    if ( ! obj.hasNonNullKey(label)) {
        return;
    }
    PlannerDomValue arrayObj = obj.valueForKey(label);
    for (int i = 0; i < arrayObj.arrayLen(); i++) {
        AbstractExpression *expr = AbstractExpression::buildExpressionTree(arrayObj.valueAtIndex(i));
        push_back(expr);
    }
}

void AbstractPlanNode::loadSortListFromJSONObject(PlannerDomValue obj,
                                                      std::vector<AbstractExpression*> *sortExprs,
                                                      std::vector<SortDirectionType>   *sortDirs) {
    PlannerDomValue sortColumnsArray = obj.valueForKey("SORT_COLUMNS");

    for (int i = 0; i < sortColumnsArray.arrayLen(); i++) {
        PlannerDomValue sortColumn = sortColumnsArray.valueAtIndex(i);
        bool hasDirection = (sortDirs == NULL), hasExpression = (sortExprs == NULL);

        if (sortDirs && sortColumn.hasNonNullKey("SORT_DIRECTION")) {
            hasDirection = true;
            std::string sortDirectionStr = sortColumn.valueForKey("SORT_DIRECTION").asStr();
            sortDirs->push_back(stringToSortDirection(sortDirectionStr));
        }
        if (sortExprs && sortColumn.hasNonNullKey("SORT_EXPRESSION")) {
            hasExpression = true;
            PlannerDomValue exprDom = sortColumn.valueForKey("SORT_EXPRESSION");
            sortExprs->push_back(AbstractExpression::buildExpressionTree(exprDom));
        }

        if (!(hasExpression && hasDirection)) {
            throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                          "OrderByPlanNode::loadFromJSONObject:"
                                          " Does not have expression and direction.");
        }
    }
}
} // namespace voltdb
