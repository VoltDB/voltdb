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

#include <algorithm>

#include <boost/foreach.hpp>

#include "common/tabletuple.h"
#include "executors/OptimizedProjector.hpp"
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
    ProjectStep(AbstractExpression* expr, int dstFieldIndex)
        : m_dstFieldIndex(dstFieldIndex)
        , m_action(EVAL_EXPR)
        , m_params(expr, dstFieldIndex) {
        vassert(dstFieldIndex >= 0);
    }

    // The memcpy constructor
    ProjectStep(int dstFieldIndex, int srcFieldIndex,
                size_t dstOffset, size_t srcOffset, size_t numBytes)
        : m_dstFieldIndex(dstFieldIndex)
        , m_action(MEMCPY)
        , m_params(dstFieldIndex, srcFieldIndex,
                   dstOffset, srcOffset, numBytes) {
        vassert(dstFieldIndex >= 0);
    }

    // Perform this step on the destination tuple
    void exec(TableTuple& dstTuple, const TableTuple& srcTuple) const;

    // Returns true if this is a memcpy step
    bool isMemcpy() const {
        return m_action == MEMCPY;
    }

    // Returns true if this is an expression evaluation step
    bool isEvalExpr() const {
        return m_action == EVAL_EXPR;
    }

    // Return the expression for an expression eval step.
    // Asserts if this is not an expr eval step.
    AbstractExpression* expr() const {
        vassert(m_action == EVAL_EXPR);
        return m_params.m_evalParams.m_expr;
    }

    // Returns the field index in the destination tuple for this step.
    // For memcpy steps that span multiple contiguous fields, returns the
    // lowest field index.
    int dstFieldIndex() const {
        return m_dstFieldIndex;
    }

    // Returns the source index for TVE expr steps, and memcpy steps
    // that have not been coalesced, and -1 otherwise.
    int srcFieldIndex() const;

    // For memcpy steps, returns the offset in the dst tuple
    // (first argument to memcpy)
    // asserts if this is not a memcpy step
    size_t dstOffset() const {
        vassert(m_action == MEMCPY);
        return m_params.m_memcpyParams.m_dstOffset;
    }

    // For memcpy steps, returns the offset in the src tuple
    // (second argument to memcpy)
    // asserts if this is not a memcpy step
    size_t srcOffset() const {
        vassert(m_action == MEMCPY);
        return m_params.m_memcpyParams.m_srcOffset;
    }

    // For memcpy steps, returns the number of bytes to copy
    // (third argument to memcpy)
    // asserts if this is not a memcpy step
    size_t numBytes() const {
        vassert(m_action == MEMCPY);
        return m_params.m_memcpyParams.m_numBytes;
    }

    // Returns a handy string for this step
    std::string debug() const;

private:

    enum Action {
        MEMCPY,
        EVAL_EXPR
    };

    struct Memcpy {
        int m_srcFieldIndex;

        size_t m_dstOffset;
        size_t m_srcOffset;
        size_t m_numBytes;
    };

    struct Eval {
        AbstractExpression* m_expr;
    };

    union Params {

        Params(AbstractExpression* expr, int dstFieldIndex) {
            m_evalParams.m_expr = expr;
        }

        Params(int dstFieldIndex, int srcFieldIndex,
               size_t dstOffset, size_t srcOffset, size_t numBytes) {
            m_memcpyParams.m_srcFieldIndex = srcFieldIndex;

            m_memcpyParams.m_dstOffset = dstOffset;
            m_memcpyParams.m_srcOffset = srcOffset;
            m_memcpyParams.m_numBytes = numBytes;
        }

        Memcpy m_memcpyParams;
        Eval m_evalParams;
    };

    int m_dstFieldIndex;
    Action m_action;
    Params m_params;
};

// Implement less-than.  We want to order by field index in the
// destination tuple.  Source tuple field index is not appropriate
// for ordering: fields in source tuple may be referenced more
// than once, or projection expression may not be a TVE.
struct StepComparator {
    bool operator() (const ProjectStep& lhs, const ProjectStep& rhs) const {
        return lhs.dstFieldIndex() < rhs.dstFieldIndex();
    }
};



void ProjectStep::exec(TableTuple& dstTuple, const TableTuple& srcTuple) const {
    switch (m_action) {
    case MEMCPY: {
        Memcpy memcpyParams = m_params.m_memcpyParams;
        ::memcpy(dstTuple.address() + TUPLE_HEADER_SIZE + memcpyParams.m_dstOffset,
                 srcTuple.address() + TUPLE_HEADER_SIZE + memcpyParams.m_srcOffset,
                 memcpyParams.m_numBytes);
        break;
    }
    case EVAL_EXPR: {
        Eval evalParams = m_params.m_evalParams;
        dstTuple.setNValue(m_dstFieldIndex,
                           evalParams.m_expr->eval(&srcTuple, NULL));
        break;
    }
    default:
        vassert(false);
    }
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
        return m_params.m_memcpyParams.m_srcFieldIndex;
    default:
        vassert(false);
    }

    return -1;
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
        vassert(false);
    }

    return oss.str();
}

std::string OptimizedProjector::debug(const std::string& title) const {
    std::ostringstream oss;
    oss << "\n" << title << " steps:\n";
    BOOST_FOREACH(const ProjectStep& step, *m_steps) {
        oss << "  " << step.debug() << "\n";
    }
    oss << "\n";
    return oss.str();
}

static uint32_t getNumBytesForMemcpy(const TupleSchema::ColumnInfo* colInfo) {

    // For varialble length data, we always copy the max number of
    // bytes, even though the actual value may be shorter.  This
    // simplifies logic at runtime.
    //
    // Inlined variable length data has a 1-byte size prefix.

    switch (colInfo->getVoltType()) {
        case ValueType::tVARCHAR:
            if (colInfo->inlined && !colInfo->inBytes) {
                // For VARCHAR we need to consider multi-byte characters.
                uint32_t maxLength = colInfo->length * MAX_BYTES_PER_UTF8_CHARACTER;
                vassert(maxLength < UNINLINEABLE_OBJECT_LENGTH);
                return maxLength + 1;
            }

            // FALL THROUGH

        case ValueType::tVARBINARY:
        case ValueType::tGEOGRAPHY:
            if (colInfo->inlined) {
                vassert(colInfo->getVoltType() != ValueType::tGEOGRAPHY);
                return colInfo->length + 1;
            } else {
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

            if (dstColInfo->getVoltType() != srcColInfo->getVoltType()
                || dstColInfo->length != srcColInfo->length
                || dstColInfo->inBytes != srcColInfo->inBytes) {
                // Implicit cast, fall back to normal eval
                outSteps.insert(step);
                continue;
            }

            vassert(dstColInfo->length == srcColInfo->length);

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

    vassert(!steps.empty());

    if (steps.size() == 1) {
        return *(steps.begin());
    }

    const ProjectStep &lastStep = *(steps.rbegin());
    const ProjectStep &firstStep = *(steps.begin());

    size_t dstOffsetDiff = (lastStep.dstOffset() - firstStep.dstOffset());
    vassert(dstOffsetDiff == (lastStep.srcOffset() - firstStep.srcOffset()));

    size_t numBytes = dstOffsetDiff + lastStep.numBytes();

    return ProjectStep(firstStep.dstFieldIndex(), -1,
                       firstStep.dstOffset(), firstStep.srcOffset(), numBytes);
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
        vassert(step.dstFieldIndex() != -1);

        if (! step.isMemcpy()) {
            outputSteps.insert(step);
            continue;
        }

        // At this point all mem copies correspond to an instance of TVE,
        // so the src field should be specified.
        vassert(step.srcFieldIndex() != -1);

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

void OptimizedProjector::permuteOnIndexBitForTest(int numBits, int bitToFlip) {

    vassert(bitToFlip >= 0);

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

std::vector<AbstractExpression*> OptimizedProjector::exprsForTest() const {
    std::vector<AbstractExpression*> theExprs;
    BOOST_FOREACH(const ProjectStep& step, *m_steps) {
        theExprs.push_back(step.expr());
    }

    return theExprs;
}

} // end namespace voltdb
