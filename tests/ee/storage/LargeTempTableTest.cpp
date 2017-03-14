/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

#include <string>
#include <vector>

#include "harness.h"

#include "common/TupleSchemaBuilder.h"
#include "common/types.h"
#include "storage/LargeTempTable.h"
#include "storage/tablefactory.h"

using namespace voltdb;

class LargeTempTableTest : public Test {

};

TEST_F(LargeTempTableTest, Basic) {
    ASSERT_EQ(1, 1);
    TupleSchemaBuilder schemaBuilder(3);

    schemaBuilder.setColumnAtIndex(0, VALUE_TYPE_BIGINT);
    schemaBuilder.setColumnAtIndex(1, VALUE_TYPE_DOUBLE);
    schemaBuilder.setColumnAtIndex(2, VALUE_TYPE_VARCHAR, 128);
    std::vector<std::string> names{"pk", "val", "text"};

    voltdb::LargeTempTable *ltt = voltdb::TableFactory::buildLargeTempTable(
        "ltmp",
        schemaBuilder.build(),
        names);

    (void)ltt;
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
