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

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.voltdb.MockRow;
import org.voltdb.VoltTableRow;
import java.util.Arrays;
import java.util.OptionalInt;

public class VoltRowAssertion extends AbstractAssert<VoltRowAssertion, VoltTableRow> {
    protected VoltRowAssertion(VoltTableRow voltTableRow) {
        super(voltTableRow, VoltRowAssertion.class);
    }

    public static VoltRowAssertion assertThat(VoltTableRow actual) {
        return new VoltRowAssertion(actual);
    }

    private OptionalInt rowIndexWhenFailed = OptionalInt.empty();

    public void isEqualTo(MockRow actual) {
        containing(actual.getValues());
    }

    public VoltRowAssertion printRowIndexOnFailure(int index) {
        this.rowIndexWhenFailed = OptionalInt.of(index);
        return this;
    }

    public void containing(Object... expected) {
        Assertions
                .assertThat(actual.getColumnCount())
                .overridingErrorMessage(createErrorMessage(expected))
                .isEqualTo(expected.length);

        for (int i = 0; i < expected.length; i++) {
            Object expectedColumnValue = expected[i];
            Object actualColumnValue = actual.get(i);

            Assertions.assertThat(actualColumnValue)
                      .overridingErrorMessage(
                              createErrorMessage(rowIndexWhenFailed, i, expectedColumnValue, actualColumnValue))
                      .isEqualTo(expectedColumnValue);
        }
    }

    private String createErrorMessage(
            OptionalInt rowIndexWhenFailed,
            int columnIndex,
            Object expectedColumnValue,
            Object actualColumnValue
    ) {
        String template = "Column values at index %d are not equal. Expected <%s>(%s), actual <%s>(%s)";
        if (rowIndexWhenFailed.isPresent()) {
            template = "Mismatch at row " + rowIndexWhenFailed.getAsInt() + ". " + template;
        }

        return String.format(template,
                             columnIndex,
                             expectedColumnValue,
                             expectedColumnValue.getClass().getCanonicalName(),
                             actualColumnValue,
                             actualColumnValue.getClass().getCanonicalName()
        );
    }

    private String createErrorMessage(Object[] expected) {
        String template = "Expected column count not equal to actual. Expected <%d>, actual <%d>. Expected contents <%s>, actual <%s>.";
        if (rowIndexWhenFailed.isPresent()) {
            template = "Mismatch at row " + rowIndexWhenFailed.getAsInt() + ". " + template;
        }

        return String.format(template,
                             expected.length,
                             actual.getColumnCount(),
                             Arrays.toString(expected),
                             actual.getRow()
        );
    }
}
