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

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.voltdb.MockRow;
import org.voltdb.VoltTable;

public class VoltTableAssertion extends AbstractAssert<VoltTableAssertion, VoltTable> {
    protected VoltTableAssertion(VoltTable voltTable) {
        super(voltTable, VoltTableAssertion.class);
    }

    public static VoltTableAssertion assertThat(VoltTable actual) {
        return new VoltTableAssertion(actual);
    }

    public VoltTableAssertion hasAtLeastOneRow() {
        Assertions.assertThat(actual.getRowCount()).isGreaterThan(0);
        return this;
    }

    public VoltRowAssertion hasOneRow() {
        Assertions.assertThat(actual.getRowCount()).isEqualTo(1);
        return new VoltRowAssertion(actual.fetchRow(0));
    }

    public VoltTableAssertion isEmpty() {
        Assertions
                .assertThat(actual.getRowCount())
                .overridingErrorMessage("Expected empty table but there are <%d> rows.", actual.getRowCount())
                .isZero();
        return this;
    }

    public void containsExactly(MockRow... expected) {
        Assertions.assertThat(expected)
                  .isNotNull()
                  .doesNotContainNull();

        Assertions.assertThat(actual.getRowCount())
                  .overridingErrorMessage(
                          "Row count differs. Expected <%d>, actual <%d>",
                          expected.length,
                          actual.getRowCount()

                  )
                  .isEqualTo(expected.length);

        UnmodifiableIterator<MockRow> actualIterator = Iterators.forArray(expected);

        actual.resetRowPosition();
        while (actual.advanceRow()) {
            int activeRowIndex = actual.getActiveRowIndex();

            VoltRowAssertion
                    .assertThat(actual.fetchRow(activeRowIndex))
                    .printRowIndexOnFailure(activeRowIndex)
                    .isEqualTo(actualIterator.next());
        }
    }
}
