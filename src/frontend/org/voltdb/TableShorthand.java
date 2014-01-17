/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

package org.voltdb;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Given a string shorthand for CREATE table, create a VoltTable
 * that has the desired schema, and annotate it with enough metadata
 * to do two things:
 * 1) Create the "CREATE TABLE..." statement needed to create the table.
 * 2) Enforce column widths for strings and varbinaries.
 * Note that no SQL constraints like unique or not null will be enforced.
 *
 * Syntax is:
 * [TABLENAME] (COLUMN1, COLUMN2, .. COLUMNN) [P(PKEYCOL1, PKEYCOL2, .. PKEYCOLN)]
 * Where COLUMNX is of the form:
 * [NAME:]TYPE[SIZE]-[U][N][/'default value']
 * And PKEYCOLX is either the column index or name.
 *
 * Unnamed tables are named "T". Unnamed columns are named "CX" where X is their 0-based-index.
 * The U or N after the dash imply unique and not null respectively.
 * Default values are provided by / followed by a single-quoted string with no way to escape.
 * Unspecified lengths for VARCHAR or VARBINARY types default to 255.
 *
 * The simplest possible table would be something like:
 * "(BIGINT)"
 * Which would lead to "CREATE TABLE T (C0 BIGINT);"
 *
 * A more complex example might be:
 * "FOO (BIGINT-N, BAR:TINYINT, A:VARCHAR12-U/'foo') P(2,BAR)"
 * Which creates:
 * CREATE TABLE FOO (
 *   C0 BIGINT NOT NULL,
 *   BAR TINYINT,
 *   A VARCHAR(12) UNIQUE DEFAULT 'foo',
 *   PRIMARY KEY (A,BAR)
 * );
 *
 * Test cases are in org.voltdb.TestTableHelper
 */
public class TableShorthand {

    static Pattern m_namePattern;
    static Pattern m_columnsPattern;
    static Pattern m_pkeyPattern;
    static Pattern m_colTypePattern;
    static Pattern m_colSizePattern;
    static Pattern m_colMetaPattern;
    static Pattern m_colDefaultPattern;

    // init static regex patterns in a static block that prints stacks on failure
    // otherwise it's very hard to debug static initializers when your regex is
    // messed up
    static {
        try {
            m_namePattern = Pattern.compile("^\\w*(?=\\s+\\()");
            m_columnsPattern = Pattern.compile("(?<=\\()[^\\)]*(?=\\))");
            m_pkeyPattern = Pattern.compile("(?<=P\\()[^\\)]*(?=\\))");
            m_colTypePattern = Pattern.compile("^[A-Za-z]*");
            m_colSizePattern = Pattern.compile("(?<=[A-Za-z])\\d+");
            m_colMetaPattern = Pattern.compile("-[A-Za-z]*");
            m_colDefaultPattern = Pattern.compile("(?<=/[\\s*'])[^']*(?=')");
        }
        catch (Exception e) {
            e.printStackTrace();
            assert(false);
        }
    }

    /**
     * Extend VoltTable.ColumnInfo with code to move to parse shorthand
     */
    static class ColMeta extends VoltTable.ColumnInfo {

        ColMeta(String name, VoltType type) {
            super(name, type);
        }

        static ColMeta parse(String colShorthand, int index) {
            String name;
            VoltType type = VoltType.BIGINT;
            int size = 255;
            boolean unique = false;
            boolean nullable = true;
            String defaultValue = VoltTable.ColumnInfo.NO_DEFAULT_VALUE; // stupid reserved work

            try {
                String[] parts = colShorthand.trim().split(":", 2);
                if (parts.length > 2) throw new Exception();

                // name
                if (parts.length == 2) name = parts[0].trim();
                else name = "C" + String.valueOf(index);
                String rest = parts[parts.length - 1].trim();

                // type (required)
                Matcher typeMatcher = m_colTypePattern.matcher(rest);
                typeMatcher.find();
                type = VoltType.typeFromString(typeMatcher.group());

                // size
                Matcher sizeMatcher = m_colSizePattern.matcher(rest);
                if (sizeMatcher.find()) {
                    String val = sizeMatcher.group();
                    if (val.length() > 0) {
                        size = Integer.parseInt(sizeMatcher.group());
                    }
                }

                // flags
                Matcher metaMatcher = m_colMetaPattern.matcher(rest);
                if (metaMatcher.find()) {
                    String meta = metaMatcher.group().toUpperCase();
                    if (meta.contains("N")) nullable = false;
                    if (meta.contains("U")) unique = true;
                }

                // default value
                Matcher defaultMatcher = m_colDefaultPattern.matcher(rest);
                if (defaultMatcher.find()) {
                    defaultValue = defaultMatcher.group();
                }
            }
            catch (Exception e) {
                String msg = String.format("Parse error while parsing column %d", index);
                throw new RuntimeException(msg, e);
            }

            ColMeta column = new ColMeta(name, type);
            column.defaultValue = defaultValue;
            column.nullable = nullable;
            column.size = size;
            column.unique = unique;

            return column;
        }
    }

    /**
     * Parse the shorthand according to the syntax as described
     * in the class comment.
     */
    public static VoltTable tableFromShorthand(String schema) {

        String name = "T";
        ColMeta[] columns = null;

        // get a name
        Matcher nameMatcher = m_namePattern.matcher(schema);
        if (nameMatcher.find()) {
            name = nameMatcher.group().trim();
        }

        // get the column schema
        Matcher columnDataMatcher = m_columnsPattern.matcher(schema);
        if (!columnDataMatcher.find()) {
            throw new IllegalArgumentException("No column data found in shorthand");
        }
        String[] columnData = columnDataMatcher.group().trim().split("\\s*,\\s*");
        int columnCount = columnData.length;

        columns = new ColMeta[columnCount];

        for (int i = 0; i < columnCount; i++) {
            columns[i] = ColMeta.parse(columnData[i], i);
        }

        // get the pkey
        Matcher pkeyMatcher = m_pkeyPattern.matcher(schema);
        if (pkeyMatcher.find()) {
            String[] pkeyColData = pkeyMatcher.group().trim().split("\\s*,\\s*");
            for (int pkeyIndex = 0; pkeyIndex < pkeyColData.length; pkeyIndex++) {
                String pkeyCol = pkeyColData[pkeyIndex];
                // numeric means index of column
                if (Character.isDigit(pkeyCol.charAt(0))) {
                    int colIndex = Integer.parseInt(pkeyCol);
                    columns[colIndex].pkeyIndex = pkeyIndex;
                }
                else {
                    for (int colIndex = 0; colIndex < columnCount; colIndex++) {
                        if (columns[colIndex].name.equals(pkeyCol)) {
                            columns[colIndex].pkeyIndex = pkeyIndex;
                            break;
                        }
                    }
                }
            }
        }

        VoltTable table = new VoltTable(columns);
        table.m_name = name;
        return table;
    }
}
