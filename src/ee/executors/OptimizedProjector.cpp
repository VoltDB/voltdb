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


#include <algorithm>

#include "boost/foreach.hpp"

#include "common/tabletuple.h"
#include "executors/OptimizedProjector.hpp"
#include "expressions/abstractexpression.h"
#include "expressions/tuplevalueexpression.h"

namespace voltdb {

// This class is basically just a tagged union with some accessors provided.
// And also an "exec" method that performs the step.
//
// A ProjectStep is either an expression to be evaluated, and stored in
// the destination tuple, or a memcpy of one or more contiguous fields into
// the destination tuple's memory.
class ProjectStep {
public:

    // expression evaluation constructor
    ProjectStep(AbstractExpression* expr, int dstFieldIndex);

    // The memcpy constructor
    ProjectStep(int dstFieldIndex, int srcFieldIndex,
                size_t dstOffset, size_t srcOffset, size_t numBytes);

    // Perform this step on the destination tuple
    void exec(TableTuple& dstTuple, const TableTuple& srcTuple) const;

    // Returns true if this is a memcpy step
    bool isMemcpy() const;

    // Returns true if this is an expression evaluation step
    bool isEvalExpr() const;

    // Return the expression for an expression eval step.
    // Asserts if this is not an expr eval step.
    AbstractExpression* expr() const;

    // Returns the field index in the destination tuple for this step.
    // For memcpy steps that span multiple contiguous fields, returns the
    // lowest field index.
    int dstFieldIndex() const;

    // Returns the source index for TVE expr steps, and memcpy steps
    // that have not been coalesced, and -1 otherwise.
    int srcFieldIndex() const;

    // For memcpy steps, returns the offset in the dst tuple
    // (first argument to memcpy)
    // asserts if this is not a memcpy step
    size_t dstOffset() const;

    // For memcpy steps, returns the offset in the src tuple
    // (second argument to memcpy)
    // asserts if this is not a memcpy step
    size_t srcOffset() const;

    // For memcpy steps, returns the number of bytes to copy
    // (third argument to memcpy)
    // asserts if this is not a memcpy step
    size_t numBytes() const;

    // Returns a handy string for this step
    std::string debug() const;

private:

    enum Action {
        MEMCPY,
        EVAL_EXPR
    } m_action;

    union Params {

        Params(AbstractExpression* expr, int dstFieldIndex);

        Params(int dstFieldIndex, int srcFieldIndex, size_t dstOffset, size_t srcOffset, size_t numBytes);

        struct Memcpy {
            int dstFieldIndex;
            int srcFieldIndex;

            size_t dstOffset;
            size_t srcOffset;
            size_t numBytes;
        } memcpyParams;

        struct Eval {
            AbstractExpression* expr;
            int dstFieldIndex;
        } evalParams;

    };

    Params m_params;
};

// Implement less than.  We want to order by field index in the
// destination tuple.  Source tuple field index is not appropriate
// for ordering: fields in source tuple may be referenced more
// than once, or projection expression may not be a TVE.
struct StepComparator {
    bool operator() (const ProjectStep& lhs, const ProjectStep& rhs);
};

// expression evaluation constructor
ProjectStep::ProjectStep(AbstractExpression* expr, int dstFieldIndex)
    : m_action(EVAL_EXPR)
    , m_params(expr, dstFieldIndex)
{
    assert (dstFieldIndex >= 0);
}

// The memcpy constructor
ProjectStep::ProjectStep(int dstFieldIndex, int srcFieldIndex,
                         size_t dstOffset, size_t srcOffset, size_t numBytes)
    : m_action(MEMCPY)
    , m_params(dstFieldIndex, srcFieldIndex,
               dstOffset, srcOffset, numBytes)
{
    assert (dstFieldIndex >= 0);
}


void ProjectStep::exec(TableTuple& dstTuple, const TableTuple& srcTuple) const {
    switch (m_action) {
    case MEMCPY: {
        Params::Memcpy memcpyParams = m_params.memcpyParams;
        ::memcpy(dstTuple.address() + TUPLE_HEADER_SIZE + memcpyParams.dstOffset,
                 srcTuple.address() + TUPLE_HEADER_SIZE + memcpyParams.srcOffset,
                 memcpyParams.numBytes);
        break;
    }
    case EVAL_EXPR: {
        Params::Eval evalParams = m_params.evalParams;
        dstTuple.setNValue(evalParams.dstFieldIndex,
                           evalParams.expr->eval(&srcTuple, NULL));
        break;
    }
    default:
        assert(false);
    }
}

bool ProjectStep::isMemcpy() const {
    return m_action == MEMCPY;
}

bool ProjectStep::isEvalExpr() const {
    return m_action == EVAL_EXPR;
}

AbstractExpression* ProjectStep::expr() const {
    assert (m_action == EVAL_EXPR);
    return m_params.evalParams.expr;
}

int ProjectStep::dstFieldIndex() const {
    switch (m_action) {
    case EVAL_EXPR:
        return m_params.evalParams.dstFieldIndex;
    case MEMCPY:
        return m_params.memcpyParams.dstFieldIndex;
    default:
        assert(false);
    }

    return -1;
}

int ProjectStep::srcFieldIndex() const {
    switch (m_action) {
    case EVAL_EXPR:
        if (expr()->getExpressionType() == EXPRESSION_TYPE_VALUE_TUPLE) {
            const TupleValueExpression* tve = static_cast<const TupleValueExpression*>(expr());
            return tve->getColumnId();
        }
        else {
            // This expression is more complicated than just a
            // field access.
            return -1;
        }
    case MEMCPY:
        return m_params.memcpyParams.srcFieldIndex;
    default:
        assert(false);
    }

    return -1;
}

size_t ProjectStep::dstOffset() const {
    assert (m_action == MEMCPY);
    return m_params.memcpyParams.dstOffset;
}

size_t ProjectStep::srcOffset() const {
    assert (m_action == MEMCPY);
    return m_params.memcpyParams.srcOffset;
}

size_t ProjectStep::numBytes() const {
    assert (m_action == MEMCPY);
    return m_params.memcpyParams.numBytes;
}

std::string ProjectStep::debug() const {
    std::ostringstream oss;
    switch (m_action) {
    case MEMCPY:
        oss << "MEMCPY: "
            << "dstIdx: " << dstFieldIndex() << ", srcIdx: " << srcFieldIndex()
            << ", memcpy(+" << dstOffset()
            << ", +" << srcOffset()
            << ", " << numBytes() << ")";
        break;
    case EVAL_EXPR:
        oss << "EVAL_EXPR: "
            << "dstIdx: " << dstFieldIndex() << ", srcIdx: " << srcFieldIndex()
            << ", " << expr()->debug();
        break;
    default:
        assert(false);
    }

    return oss.str();
}

ProjectStep::Params::Params(AbstractExpression* expr, int dstFieldIndex) {
    evalParams.expr = expr;
    evalParams.dstFieldIndex = dstFieldIndex;
}

ProjectStep::Params::Params(int dstFieldIndex, int srcFieldIndex,
                            size_t dstOffset, size_t srcOffset, size_t numBytes) {
    memcpyParams.dstFieldIndex = dstFieldIndex;
    memcpyParams.srcFieldIndex = srcFieldIndex;

    memcpyParams.dstOffset = dstOffset;
    memcpyParams.srcOffset = srcOffset;
    memcpyParams.numBytes = numBytes;
}

// Implement less than.  We want to order by field index in the
// destination tuple.  Source tuple field index is not appropriate
// for ordering: fields in source tuple may be referenced more
// than once, or projection expression may not be a TVE.
bool StepComparator::operator() (const ProjectStep& lhs, const ProjectStep& rhs) {
    return lhs.dstFieldIndex() < rhs.dstFieldIndex();
}

void dumpSteps(const std::string& title, const ProjectStepSet& steps) {
    std::cout << "\n" << title << " steps:\n";
    BOOST_FOREACH(const ProjectStep& step, steps) {
        std::cout << "  " << step.debug() << "\n";
    }
    std::cout << "\n";
}

static uint32_t getNumBytesForMemcpy(const TupleSchema::ColumnInfo* colInfo) {

    // For varialble length data, we always copy the max number of
    // bytes, even though the actual value may be shorter.  This
    // simplifies logic at runtime.
    //
    // Inlined variable length data has a 1-byte size prefix.

    switch (colInfo->getVoltType()) {
    case VALUE_TYPE_VARCHAR:
        if (colInfo->inlined && !colInfo->inBytes) {
            // For VARCHAR we need to consider multi-byte characters.
            uint32_t maxLength = colInfo->length * MAX_BYTES_PER_UTF8_CHARACTER;
            assert (maxLength < UNINLINEABLE_OBJECT_LENGTH);
            return maxLength + 1;
        }

        // FALL THROUGH

    case VALUE_TYPE_VARBINARY:
        if (colInfo->inlined) {
            return colInfo->length + 1;
        }
        else {
            return sizeof (StringRef**);
        }
    default:
        return colInfo->length;
    }
}

// Any TVEs in the step set will be converted to mem copies.  This is
// faster since it avoids the overhead of serializing and deserializing
// an NValue.
static ProjectStepSet convertTVEsToMemcpy(const TupleSchema* dstSchema,
                                          const TupleSchema* srcSchema,
                                          const ProjectStepSet& steps) {
    ProjectStepSet outSteps;
    BOOST_FOREACH(const ProjectStep& step, steps) {
        AbstractExpression *e = step.expr();
        if (e->getExpressionType() == EXPRESSION_TYPE_VALUE_TUPLE) {
            const TupleSchema::ColumnInfo* dstColInfo = dstSchema->getColumnInfo(step.dstFieldIndex());
            const TupleSchema::ColumnInfo* srcColInfo = srcSchema->getColumnInfo(step.srcFieldIndex());

            // XXX test this!
            if (dstColInfo->getVoltType() != srcColInfo->getVoltType()
                || dstColInfo->length != srcColInfo->length
                || dstColInfo->inBytes != srcColInfo->inBytes) {
                // Implicit cast, fall back to normal eval
                outSteps.insert(step);
                continue;
            }

            assert (dstColInfo->length == srcColInfo->length);

            size_t numBytes = getNumBytesForMemcpy(srcColInfo);
            ProjectStep memcpyStep(step.dstFieldIndex(), step.srcFieldIndex(),
                                   dstColInfo->offset, srcColInfo->offset, numBytes);
            outSteps.insert(memcpyStep);
        }
        else {
            // non TVEs cannot be replaced with memcpy.
            outSteps.insert(step);
        }
    }

    return outSteps;
}

// Given a set of contiguous memcpy steps, return a single step
// that does the same thing.
static ProjectStep squishSteps(const ProjectStepSet& steps) {

    assert (!steps.empty());

    if (steps.size() == 1) {
        return *(steps.begin());
    }

    size_t dstOffset = steps.begin()->dstOffset();
    size_t srcOffset = steps.begin()->srcOffset();
    size_t numBytes = (steps.rbegin()->dstOffset() - steps.begin()->dstOffset()) + steps.rbegin()->numBytes();



    return ProjectStep(steps.begin()->dstFieldIndex(), -1, dstOffset, srcOffset, numBytes);
}

// Given a set of steps where all the TVEs have been converted to mem copies,
// Return a new set of steps with the adjacent mem copies (adjacent in both src and
// destination) coalesced into one memcpy step.
static ProjectStepSet coalesceMemcpys(const TupleSchema* dstSchema,
                                      const TupleSchema* srcSchema,
                                      const ProjectStepSet& steps) {
    ProjectStepSet outputSteps;
    ProjectStepSet inProgressGroup;

    BOOST_FOREACH(const ProjectStep& step, steps) {
        assert (step.dstFieldIndex() != -1);

        if (! step.isMemcpy()) {
            outputSteps.insert(step);
            continue;
        }

        // At this point all mem copies correspond to an instance of TVE,
        // so the src field should be specified.
        assert (step.srcFieldIndex() != -1);

        if (inProgressGroup.empty()) {
            inProgressGroup.insert(step);
        }
        else if ((step.dstFieldIndex() == inProgressGroup.rbegin()->dstFieldIndex() + 1)
                 && (step.srcFieldIndex() == inProgressGroup.rbegin()->srcFieldIndex() + 1)) {
            // This step is a continuation of a continguous group.
            inProgressGroup.insert(step);
        }
        else {
            // This step starts a new potentially contiguous group.

            // squish the old contiguous group and add it
            // to the output
            outputSteps.insert(squishSteps(inProgressGroup));

            // Start a new in progress group
            inProgressGroup.clear();
            inProgressGroup.insert(step);
        }
    }

    if (! inProgressGroup.empty()) {
        outputSteps.insert(squishSteps(inProgressGroup));
    }

    return outputSteps;
}

OptimizedProjector::OptimizedProjector(const std::vector<AbstractExpression*>& exprs)
    : m_steps(new ProjectStepSet())
{
    int i = 0;
    BOOST_FOREACH(AbstractExpression *e, exprs) {
        insertStep(e, i);
        ++i;
    }
}

OptimizedProjector::OptimizedProjector()
    : m_steps(new ProjectStepSet())
{
}

OptimizedProjector::OptimizedProjector(const OptimizedProjector& that)
    : m_steps(new ProjectStepSet(*that.m_steps))
{
}

OptimizedProjector& OptimizedProjector::operator= (const OptimizedProjector& rhs) {
    OptimizedProjector rhsCopy(rhs);

    m_steps.swap(rhsCopy.m_steps);

    return *this;
}

OptimizedProjector::~OptimizedProjector() {
}

void OptimizedProjector::insertStep(AbstractExpression* expr,
                                    int dstFieldIndex) {
    m_steps->insert(ProjectStep(expr, dstFieldIndex));
}

void OptimizedProjector::optimize(const TupleSchema* dstSchema,
                                  const TupleSchema* srcSchema) {
    ProjectStepSet memcpySteps = convertTVEsToMemcpy(dstSchema,
                                                     srcSchema,
                                                     *m_steps);
    m_steps.reset(new ProjectStepSet(coalesceMemcpys(dstSchema, srcSchema, memcpySteps)));
}

void OptimizedProjector::exec(TableTuple& dstTuple, const TableTuple& srcTuple) const {
    BOOST_FOREACH(const ProjectStep& step, *m_steps) {
        step.exec(dstTuple, srcTuple);
    }
}

void OptimizedProjector::permuteOnIndexBit(int numBits, int bitToFlip) {

    assert (bitToFlip >= 0);

    if (bitToFlip >= numBits) {
        return;
    }

    ProjectStepSet permutedSteps;

    BOOST_FOREACH(const ProjectStep& step, *m_steps) {
        int dstFieldIndex = step.dstFieldIndex();

        dstFieldIndex ^= 1 << bitToFlip;
        permutedSteps.insert(ProjectStep(step.expr(), dstFieldIndex));
    }

    m_steps.reset(new ProjectStepSet(permutedSteps));
}

size_t OptimizedProjector::numSteps() const {
    return m_steps->size();
}

std::vector<AbstractExpression*> OptimizedProjector::exprs() const {
    std::vector<AbstractExpression*> theExprs;
    BOOST_FOREACH(const ProjectStep& step, *m_steps) {
        theExprs.push_back(step.expr());
    }

    return theExprs;
}

} // end namespace voltdb
