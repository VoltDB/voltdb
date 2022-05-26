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

#include <set>
#include <vector>

#include "boost/scoped_ptr.hpp"

#include "expressions/abstractexpression.h"

namespace voltdb {

// Forward declarations
class TableTuple;
class TupleSchema;
class ProjectStep;
struct StepComparator;
typedef std::set<ProjectStep, StepComparator> ProjectStepSet;

/**
 * A class that accepts a list of expressions to be projected
 * into a temp table, and produces (internally) a set of steps
 * to perform the projection.  Tuple value expressions are
 * replaced with memcpys.  Where possible, copies of adjacent
 * fields are coalesced into a single call to memcpy.
 */
class OptimizedProjector {
    boost::scoped_ptr<ProjectStepSet> m_steps;
public:

    /** Produce an optimized projector for the given set of expressions.
     * Expressions are assumed to be in the order they will be placed in the
     * destination tuple.  I.e., exprs[0] will go into the first field, etc.
     *
     * To get the optimized projection, call the optimize method before
     * calling exec.
     */
    OptimizedProjector(const std::vector<AbstractExpression*>& exprs);

    /** Default constructor.  Produces an empty Projector that does nothing. */
    OptimizedProjector();

    /** Copy constructor. */
    OptimizedProjector(const OptimizedProjector& that);

    /** This destructor is required for forward declarations to be useful */
    ~OptimizedProjector();

    /** Assignment operator */
    OptimizedProjector& operator=(const OptimizedProjector& rhs);

    /** Add a step to this projection */
    void insertStep(AbstractExpression *expr, int dstFieldIndex);

    /** Optimize the projection into as few mem copies as possible */
    void optimize(const TupleSchema* dstSchema, const TupleSchema* srcSchema);

    /** Perform the projection on a destination tuple. */
    void exec(TableTuple& dstTuple, const TableTuple& srcTuple) const;

    /** The number of steps needed to perform this projection */
    size_t numSteps() const;

    /** For testing, return an expression for each step.  (This must
     * be done before optimizing, before expressions are replaced with
     * mem copies.)*/
    std::vector<AbstractExpression*> exprsForTest() const;

    /** For testing, re-order the target fields so mem copies must be
     *  broken up. */
    void permuteOnIndexBitForTest(int numBits, int bitToFlip);

    std::string debug(const std::string &title) const;

};

} // end namespace voltdb

