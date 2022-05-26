/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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
package org.voltdb.assertions;

import org.assertj.core.api.AbstractObjectArrayAssert;
import org.assertj.core.api.Assertions;
import org.voltdb.VoltTable;

public class VoltTablesAssertion extends AbstractObjectArrayAssert<VoltTablesAssertion, VoltTable> {
    public static VoltTablesAssertion assertThat(VoltTable[] actual) {
        return new VoltTablesAssertion(actual);
    }

    protected VoltTablesAssertion(VoltTable[] actual) {
        super(actual, VoltTablesAssertion.class);
    }

    @Override
    protected VoltTablesAssertion newObjectArrayAssert(VoltTable[] voltTables) {
        return assertThat(voltTables);
    }

    public VoltTablesAssertion hasAtLeastOneResponse() {
        Assertions.assertThat(actual).allMatch(table -> table.getRowCount() > 0);
        return this;
    }

    public VoltTableAssertion onlyResponse() {
        Assertions.assertThat(actual).hasSize(1);
        return VoltTableAssertion.assertThat(actual[0]);
    }

    public VoltTableAssertion first() {
        hasAtLeastOneResponse();
        return VoltTableAssertion.assertThat(actual[0]);
    }

    public void isEmpty() {
        Assertions.assertThat(actual).isEmpty();
    }
}
