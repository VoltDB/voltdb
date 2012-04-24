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

#ifndef VOLTDBNODEABSTRACTEXECUTOR_H
#define VOLTDBNODEABSTRACTEXECUTOR_H

#include "plannodes/abstractplannode.h"
#include "storage/temptable.h"
#include <cassert>

namespace voltdb {

class TempTableLimits;
class VoltDBEngine;

/**
 * AbstractExecutor provides the API for initializing and invoking executors.
 */
class AbstractExecutor {
  public:
    virtual ~AbstractExecutor();

    /** Executors are initialized once when the catalog is loaded */
    bool init(VoltDBEngine*, TempTableLimits* limits);

    /** Invoke a plannode's associated executor */
    bool execute(const NValueArray& params);

    /**
     * Returns true if the output table for the plannode must be cleaned up
     * after p_execute().  <b>Default is false</b>. This should be overriden in
     * the receive executor since this is the only place we need to clean up the
     * output table.
     */
    virtual bool needsPostExecuteClear() { return false; }

    /**
     * Returns the plannode that generated this executor.
     */
    inline AbstractPlanNode* getPlanNode() { return m_abstractNode; }

  protected:
    AbstractExecutor(VoltDBEngine* engine, AbstractPlanNode* abstractNode) {
        m_abstractNode = abstractNode;
        m_tmpOutputTable = NULL;
    }

    /** Concrete executor classes implement initialization in p_init() */
    virtual bool p_init(AbstractPlanNode*,
                        TempTableLimits* limits) = 0;

    /** Concrete executor classes impelmenet execution in p_execute() */
    virtual bool p_execute(const NValueArray& params) = 0;

    /**
     * Returns true if the output table for the plannode must be
     * cleared before p_execute().  <b>Default is true (clear each
     * time)</b>. Override this method if the executor receives a
     * plannode instance that must not be cleared.
     * @return true if output table must be cleared; false otherwise.
     */
    virtual bool needsOutputTableClear() { return true; };

    // execution engine owns the plannode allocation.
    AbstractPlanNode* m_abstractNode;
    TempTable* m_tmpOutputTable;

    // cache to avoid runtime virtual function call
    bool needs_outputtable_clear_cached;
    
    //@TODO pullexec prototype
  public:  
    /** Invoke a plannode's associated executor in pull mode */
//@TODO I don't envision a need for params in these functions (that can't be easily optimized out into p_pre_execute_pull). --paul
    bool execute_pull(const NValueArray& params);
    bool clearOutputTable_pull(const NValueArray& params);
  
  public:
    // Gets next available tuple from input table 
    // and also applies executor specific logic. 
    // Better be two separate methods
    // needs to be pure
//@TODO See the suggestion in VoltDBEngine.cpp for sub-optimal but simplifying default behavior for this function: --paul
    virtual TableTuple p_next_pull(const NValueArray& params, bool& status) { return TableTuple(); }
    // Returns True if executor and all its children are pull enabled
    // needs to be pure
    virtual bool is_enabled_pull(const NValueArray&) const { return false; }
    
  //protected:

//@TODO See the suggestion in VoltDBEngine.cpp for sub-optimal but simplifying default behavior for this function: --paul
    // Last minute init before the p_next_pull iteration
    // needs to be pure
    virtual bool p_pre_execute_pull(const NValueArray& params) { return false; }

//@TODO Would this make sense to default to the pass-through recursive call? --paul
    // Cleans up after the p_next_pull iteration
    // needs to be pure
    virtual bool p_post_execute_pull(const NValueArray& params) { return false; }
    
    // Saves processed tuple
    // needs to be pure
//@TODO Would it be possible to have a "smarter" default implementation for this that generically inserted into a node's (optional) output table? --paul
// Specific executors could STILL refine/override that behavior.
    virtual bool p_insert_output_table_pull(TableTuple& tuple) { return false; }
    
    /*
//@TODO These would be slightly simpler as member functions that always operated on m_abstractNode instead of always having that exact value passed into them. --paul
    // Corresponding static methods to recurs to children
    // Need generic method
    static bool p_pre_execute_pull(AbstractPlanNode* node, const NValueArray& params);
//@TODO I don't envision a need for params in these functions (that can't be easily optimized out). --paul
    static bool p_post_execute_pull(AbstractPlanNode* node, const NValueArray& params);
    static bool p_is_enabled_pull(AbstractPlanNode* node);
    */
};

inline bool AbstractExecutor::execute(const NValueArray& params)
{
    assert(m_abstractNode);
    VOLT_TRACE("Starting execution of plannode(id=%d)...",
               m_abstractNode->getPlanNodeId());

    if (m_tmpOutputTable)
    {
        VOLT_TRACE("Clearing output table...");
        m_tmpOutputTable->deleteAllTuplesNonVirtual(false);
    }

    // run the executor
    return p_execute(params);
}

//@TODO: The details of this template function are a little at cross-purposes to my suggestions about simplifying the method signatures. --paul
// I suggest generalizing the Method parameter to allow Method m to be a generic "single-argument function object" that can be called
// via "m(executor)" and can contain/encapsulate any other arguments (like params) it may need.
// Since I don't envision specific executor classes needing to define and apply new generic abstract executor methods,
// I'd go (back) to wrapping this functionality inside easy-to-call abstract executor member functions,
// and further (in most cases) provide them as default "pass-through" implementations of virtual functions for classes that don't have
// specific behavior requirements beyond cooperation with the recursion protocol.
namespace detail {
typedef bool (AbstractExecutor::*Method)(const NValueArray&);
typedef bool (AbstractExecutor::*MethodC)(const NValueArray&) const;

template <typename Method>
bool iterate_children_pull(Method m, AbstractPlanNode* node, const NValueArray& params)
{
    assert(node != NULL);
    std::vector<AbstractPlanNode*>& children = node->getChildren();
    for (std::vector<AbstractPlanNode*>::iterator it = children.begin(); it != children.end(); ++it)
    {
        AbstractExecutor* executor = (*it)->getExecutor();
        assert(executor != NULL);
        if ((executor->*m)(params) != true)
            return false;
    }
    return true;
}

} //namespace detail 

}

#endif
