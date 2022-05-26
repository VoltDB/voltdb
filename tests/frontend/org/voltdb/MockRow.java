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
package org.voltdb;

import org.assertj.core.api.Assertions;
import org.assertj.core.data.MapEntry;
import java.util.Arrays;

public class MockRow extends VoltTableRow {

    private final VoltTable schemaTemplate;
    private final Object[] values;

    public MockRow(VoltTable schemaTemplate, Object[] values) {
        this.schemaTemplate = schemaTemplate;
        this.values = values.clone();
    }

    public static MockRow of(VoltTable schemaTemplate, Object... values) {
        Assertions
                .assertThat(schemaTemplate.getColumnCount())
                .overridingErrorMessage(
                        "Values count does not match column count from schema. Schema: <%d>, provided values: <%d>",
                        schemaTemplate.getColumnCount(),
                        values.length
                )
                .isEqualTo(values.length);

        Object[] actualTypes = Arrays
                .stream(values)
                .map(VoltType::typeFromObject)
                .toArray();

        Assertions.assertThat(schemaTemplate.getTableSchema())
                  .extracting("type")
                  .containsExactly(actualTypes);

        return new MockRow(schemaTemplate, values);
    }

    @SafeVarargs
    public static MockRow with(MockRow baseRow, MapEntry<String, Object>... newValues) {
        VoltTable schemaTemplate = baseRow.schemaTemplate;
        Object[] values = baseRow.values.clone();

        for (MapEntry<String, Object> newValue : newValues) {
            int columnIndex = schemaTemplate.getColumnIndex(newValue.getKey());
            values[columnIndex] = newValue.getValue();
        }

        return new MockRow(schemaTemplate, values);
    }

    public Object[] getValues() {
        return values;
    }

    @Override
    public VoltType getColumnType(int columnIndex) {
        return VoltType.typeFromObject(values[columnIndex]);
    }

    @Override
    public int getColumnIndex(String columnName) {
        return schemaTemplate.getColumnIndex(columnName);
    }

    @Override
    public int getColumnCount() {
        return values.length;
    }

    @Override
    int getRowCount() {
        return 1;
    }

    @Override
    int getRowStart() {
        return 0;
    }

    @Override
    byte[] getSchemaString() {
        return schemaTemplate.getSchemaString();
    }

    @Override
    public VoltTableRow cloneRow() {
        try {
            return (VoltTableRow) clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    public void insertInto(VoltTable voltTable) {
        voltTable.addRow(values);
    }
}
