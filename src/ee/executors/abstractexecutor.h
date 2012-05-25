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
#include "boost/bind.hpp"
#include "boost/function.hpp"
#include "boost/ref.hpp"
#include "boost/scoped_ptr.hpp"
#include <cassert>
#include <vector>

namespace voltdb {

class TempTableLimits;
class VoltDBEngine;

namespace detail {
struct AbstractExecutorState;
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

    /** Invoke a plannode's associated executor using the pull protocol */
    bool execute_pull(const NValueArray& params);

    /** Returns true if executor supports pull mode */
    virtual bool support_pull() const;

    // @TODO: Eventually, every executor class will have its own p_next_pull implementation
    // that operates within the pull protocol as best it can.
    // Then this function will be made abstract (pure virtual).
    // For now, AbstractExecutor provides this sub-optimal implementation that
    // relies on the executor class implementation of the older push protocol.
    // In particular, it pulls tuples from the output table that was populated by p_execute.
    /** Gets next available tuple(s) from input table as needed
     * and applies executor specific logic to produce its next tuple. */
    virtual TableTuple p_next_pull();

    /** Generic behavior wrapping the custom p_pre_execute_pull. */
    // @TODO: Does the need to prep m_tmpOutputTable really cut across executor classes?
    // Or should we consider moving this into custom p_pre_execute_pull implementations only as it may apply?
    void pre_execute_pull(const NValueArray& params);

    /** Cleans up after the p_next_pull iteration. */
    void post_execute_pull();

    /** Reset executor's pull state */
    virtual void reset_state_pull();

    // Clean up the output table of the executor tree as needed
    // Generic behavior wrapping the custom p_pre_execute_pull.
    void clearOutputTables();

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

    // Generic method to depth first iterate over the children and call the functor on each level
    // The functor signature is void f(AbstractExecutor*)
    // The second parameter controls whether the iteration should stop
    // if child doesn't support the pull mode yet or keep going. For example, if we are simply building
    // the list of all children for a given node, this parameter should be set to false.
    // It will become obsolete after all executors will be converted to the pull mode.
    template <typename Functor>
    typename Functor::result_type  depth_first_iterate_pull(Functor& f, bool stopOnPush);

  protected:
    AbstractExecutor(VoltDBEngine* engine, AbstractPlanNode* abstractNode);

  private:
    /** Concrete executor classes implement initialization in p_init() */
    virtual bool p_init(AbstractPlanNode*,
                        TempTableLimits* limits) = 0;

    /** Concrete executor classes implement the original push-based execution protocol in p_execute() */
    virtual bool p_execute(const NValueArray& params) = 0;

    /**
     * Returns true if the output table for the plannode must be
     * cleared before p_execute().  <b>Default is true (clear each
     * time)</b>. Override this method if the executor receives a
     * plannode instance that must not be cleared.
     * @return true if output table must be cleared; false otherwise.
     */
    virtual bool needsOutputTableClear() { return true; };

    // @TODO: Eventually, when every executor class has its own p_next_pull implementation,
    // p_pre_execute_pull will become abstract (pure virtual).
    // Each executor will be required to implement this function to do its own parameter processing.
    // For now, to accomodate executors that still use the push-based protocol that
    // processes parameters in p_execute, AbstractExecutor provides an implementation that
    // bridges the two protocols -- calling p_execute (and all its prerequisites) which leaves
    // results in an output table for the defaulted implementation of p_next_pull to find.
    /** Last minute init before the p_next_pull iteration */
    virtual void p_pre_execute_pull(const NValueArray& params);

    /** Executor specific logic */
    virtual void p_execute_pull();

    /** Cleans up after the p_next_pull iteration. */
    virtual void p_post_execute_pull();

    // Saves processed tuple
    virtual void p_insert_output_table_pull(TableTuple& tuple);

    // Helps clean up output tables of an executor and its dependencies.
    void clearOutputTable_pull();

  private:
    // execution engine owns the plannode allocation.
    AbstractPlanNode* m_abstractNode;
    TempTable* m_tmpOutputTable;

    // cache to avoid runtime virtual function call
    bool needs_outputtable_clear_cached;

    // @TODO: Eventually, when the p_execute push protocol is phased out
    // and each executor implements p_pre_execute_pull and p_next_pull,
    // this generic mechanism for adapting push protocol executors will not be needed.
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

inline void AbstractExecutor::p_execute_pull()
{
    // run the executor
    while (true)
    {
        // iteration stops when empty tuple is returned
        TableTuple tuple = p_next_pull();
        if (tuple.isNullTuple())
            break;
        // Insert processed tuple into the output table
        p_insert_output_table_pull(tuple);
    }
}

inline void AbstractExecutor::pre_execute_pull(const NValueArray& params) {
    assert(m_abstractNode);
    VOLT_TRACE("Starting execution of plannode(id=%d)...",
               m_abstractNode->getPlanNodeId());
    // @TODO clean-up is done twice.
    // First time here
    // Second time in VoltDBEngine::executeQuery after execute_pull is completed
    if (m_tmpOutputTable)
    {
        VOLT_TRACE("Clearing output table...");
        m_tmpOutputTable->deleteAllTuplesNonVirtual(false);
    }
    p_pre_execute_pull(params);
}

inline void AbstractExecutor::post_execute_pull() {
    p_post_execute_pull();
}

inline void AbstractExecutor::reset_state_pull() {
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
    // Applay functor to self
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
    if (needsOutputTableClear())
    {
        assert(m_tmpOutputTable);
        m_tmpOutputTable->deleteAllTuples(false);
    }
}

inline bool AbstractExecutor::support_pull() const {
    return false;
}

inline void AbstractExecutor::clearOutputTables()
{
    boost::function<void(AbstractExecutor*)> fcleanup = &AbstractExecutor::clearOutputTable_pull;
    depth_first_iterate_pull(fcleanup, false);
}

}

#endif
