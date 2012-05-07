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

#include <cassert>
#include <vector>
#include <boost/scoped_ptr.hpp>

#include "plannodes/abstractplannode.h"
#include "storage/temptable.h"

namespace voltdb {

class TempTableLimits;
class VoltDBEngine;

namespace detail
{
struct AbstractExecutorState
{   
    AbstractExecutorState(Table* table) :
        m_table(table), m_iterator(), m_list() 
    {}
    Table* m_table;
    boost::scoped_ptr<TableIterator> m_iterator;
    std::vector<AbstractExecutor*> m_list;
};

} //namespace detail

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
    AbstractExecutor(VoltDBEngine* engine, AbstractPlanNode* abstractNode);

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
    bool execute_pull(const NValueArray& params);
    
    /** Clean up the output table if needed */
    void clearOutputTable_pull();
    
    // Generic method to depth first iterate over the children 
    // and call the functor on each level 
    // The functor signature is void f(AbstractExcecutor*) 
    template <typename Functor>
    typename Functor::result_type  depth_first_iterate_pull(Functor& f, bool stopOnPush);
  
    // Gets next available tuple from input table 
    // and also applies executor specific logic. 
    // Better be two separate methods
//@TODO See the suggestion in VoltDBEngine.cpp 
// for sub-optimal but simplifying default behavior for this function: --paul
    virtual TableTuple p_next_pull();
    
    // Temp
    virtual bool support_pull() const;
    
  protected:
    
    
//@TODO See the suggestion in VoltDBEngine.cpp 
// for sub-optimal but simplifying default behavior for this function: --paul
    // Last minute init before the p_next_pull iteration
    virtual void p_pre_execute_pull(const NValueArray& params);

    // Cleans up after the p_next_pull iteration
    virtual void p_post_execute_pull();
    
    // Saves processed tuple
    virtual void p_insert_output_table_pull(TableTuple& tuple);
    
    // Recursively constructs a (depth-first) list of its child(ren).    
    void p_build_list();
    void p_add_to_list(std::vector<AbstractExecutor*>& list);
    
 protected:

    boost::scoped_ptr<detail::AbstractExecutorState> m_absState;
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

template <typename Functor>
typename Functor::result_type AbstractExecutor::depth_first_iterate_pull(
    Functor& functor, bool stopOnPush)
{
    // stop traversal if executor doesn't support pull mode. This will switch
    // it to conventional push mode. In some cases the behaviour can be overriden  
    if (this->support_pull() || !stopOnPush) { 
        assert(m_abstractNode);
        // Recurs to children and applay functor
        std::vector<AbstractPlanNode*>& children = m_abstractNode->getChildren();
        for (std::vector<AbstractPlanNode*>::iterator it = children.begin(); it != children.end(); ++it)
        {
            assert(*it);
            AbstractExecutor* executor = (*it)->getExecutor();
            assert(executor);
            executor->depth_first_iterate_pull(functor, stopOnPush);
        }
    }
    // Applay functor to itself
    return functor(this);
}

inline void AbstractExecutor::p_insert_output_table_pull(TableTuple& tuple) { 
    assert(m_tmpOutputTable);
    if (!m_tmpOutputTable->insertTuple(tuple)) {
        char message[128];
        snprintf(message, 128, "Failed to insert into table '%s'",
                m_tmpOutputTable->name().c_str());
        VOLT_ERROR("%s", message);
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      message);
     }
}

inline void AbstractExecutor::p_post_execute_pull() { 
}


inline void AbstractExecutor::clearOutputTable_pull()
{
    if (this->needsOutputTableClear())
    {
        assert(m_tmpOutputTable);
        m_tmpOutputTable->deleteAllTuples(false);
    }
}

inline bool AbstractExecutor::support_pull() const {
    return false;
}

}

#endif
