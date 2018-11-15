/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

package org.voltdb.newplanner;

import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;

import java.util.Objects;

/**
 * A base class for implementing tests against {@link VoltSqlToRelConverter}.
 *
 * @author Chao Zhou
 * @since 8.4
 */
public class VoltConverterTestCase extends VoltSqlValidatorTestCase {
    private SchemaPlus m_schemaPlus;

    /**
     * Set up m_validator and m_schemaPlus from SchemaPlus.
     *
     * @param schemaPlus
     */
    protected void init(SchemaPlus schemaPlus) {
        m_schemaPlus = schemaPlus;
        setupValidator(schemaPlus);
    }

    protected RelRoot parseValidateAndConvert(String sql) {
        Objects.requireNonNull(getValidator(), "m_validator");
        Objects.requireNonNull(m_schemaPlus, "m_schemaPlus");
        SqlNode node = parseAndValidate(sql);
        VoltSqlToRelConverter converter = VoltSqlToRelConverter.create(getValidator(), m_schemaPlus);
        RelRoot root = converter.convertQuery(node, false, true);
        root = root.withRel(converter.decorrelate(node, root.rel));
        return root;
    }
}
