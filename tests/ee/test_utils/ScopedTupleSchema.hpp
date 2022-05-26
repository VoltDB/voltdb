/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "common/TupleSchema.h"

// A class to automatically free TupleSchema instances, which cannot
// be allocated on the stack due to variable-length data that follows
// each instance.  Modeled after boost::scoped_ptr.
//
// This is not typdef'd to boost::scoped_ptr<TupleSchema> because we
// need to call freeTupleSchema to free memory for TupleSchema
// objects.
//
// In the future we should:
//   - Replace code that calls freeTupleSchema with smart pointers
//     where possible (this seems to be most uses, with a few
//     exceptions)
//   - Overload the delete operator for TupleSchema, so we can use
//     smart pointers out of the box.  I.e., the code below could be
//     replaced with
//         typedef boost::scoped_ptr<TupleSchema> ScopedTupleSchema;
class ScopedTupleSchema {
public:
    ScopedTupleSchema(voltdb::TupleSchema* schema)
        : m_schema(schema)
    {
    }

    voltdb::TupleSchema* get() {
        return m_schema;
    }

    voltdb::TupleSchema& operator*() {
        return *m_schema;
    }

    voltdb::TupleSchema* operator->() {
        return m_schema;
    }

    ~ScopedTupleSchema() {
        voltdb::TupleSchema::freeTupleSchema(m_schema);
    }

private:
    voltdb::TupleSchema* m_schema;
};
