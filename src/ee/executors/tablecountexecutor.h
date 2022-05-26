/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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


#ifndef HSTORETABLECOUNTEXECUTOR_H
#define HSTORETABLECOUNTEXECUTOR_H

#include "common/common.h"
#include "common/valuevector.h"
#include "executors/abstractexecutor.h"

namespace voltdb
{
    class TableCountExecutor : public AbstractExecutor {
    public:
        TableCountExecutor(VoltDBEngine *engine, AbstractPlanNode* abstract_node)
            : AbstractExecutor(engine, abstract_node)
        {}
        ~TableCountExecutor();
    protected:
        bool p_init(AbstractPlanNode* abstract_node,
                    const ExecutorVector& executorVector);
        bool p_execute(const NValueArray& params);
    };
}

#endif
