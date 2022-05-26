/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

#pragma once

#include "storage/table.h"

namespace voltdb {

/**
 * Base class for tables which can either be replicated or partitioned.
 *
 * All views which are added to this class are considered to be owned by this class and deleted
 * when this classes destructor is called.
 */
template<class ViewType>
class ViewableAndReplicableTable : public Table {
public:
    virtual ~ViewableAndReplicableTable() {
        // note this class has ownership of the views, even if they were allocated by VoltDBEngine
        for (auto view : m_views) {
            delete view;
        }
    }

    /**
     * @return true if this table is a replicated table
     */
    inline bool isReplicatedTable() const {
        return m_isReplicated;
    }

    /**
     * @return the TxnId which is appropriate for this table type
     */
    TransactionId getTableTxnId() const {
        return isReplicatedTable() ? m_executorContext->currentTxnId() : m_executorContext->currentSpHandle();
    }

    /*
     * claim ownership of a view. table is responsible for this view*
     */
    void addMaterializedView(ViewType* view) {
        m_views.push_back(view);
    }

    /*
     * drop a view. the table is no longer feeding it.
     * The destination table will go away when the view metadata is deleted (or later?) as its refcount goes to 0.
     */
    void dropMaterializedView(ViewType* targetView) {
        vassert( ! m_views.empty());
        ViewType* lastView = m_views.back();
        if (targetView != lastView) {
            // iterator to vector element:
            auto toView = find(m_views.begin(), m_views.end(), targetView);
            vassert(toView != m_views.end());
            // Use the last view to patch the potential hole.
            *toView = lastView;
        }
        // The last element is now excess.
        m_views.pop_back();
        delete targetView;
    }

    std::vector<ViewType*>& views() { return m_views; }

    bool hasViews() { return (m_views.size() > 0); }

protected:
    ViewableAndReplicableTable(int tableAllocationTargetSize, int partitionColumn, bool isReplicated) :
            Table(tableAllocationTargetSize),
            m_partitionColumn(partitionColumn), m_isReplicated(isReplicated), m_views() {}

    // If the table is partitioned which column it is partitioned on
    const int m_partitionColumn;

    // If true this table is replicated otherwise this table is partitioned
    const bool m_isReplicated;

    // Cached pointer to the executor context
    const ExecutorContext* m_executorContext = ExecutorContext::getExecutorContext();

    // list of materialized views that are sourced from this table
    std::vector<ViewType*> m_views;
};
}
