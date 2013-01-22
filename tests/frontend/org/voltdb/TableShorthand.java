/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TableShorthand {

    static Pattern m_namePattern;
    static Pattern m_columnsPattern;
    static Pattern m_pkeyPattern;
    static Pattern m_colTypePattern;
    static Pattern m_colSizePattern;
    static Pattern m_colMetaPattern;
    static Pattern m_colDefaultPattern;
    static {
        m_namePattern = Pattern.compile("^\\w*(?=\\s+\\()");
        m_columnsPattern = Pattern.compile("(?<=\\()[^\\)]*(?=\\))");
        m_pkeyPattern = Pattern.compile("(?<=P\\()[^\\)]*(?=\\))");
        m_colTypePattern = Pattern.compile("^[A-Za-z]*");
        m_colSizePattern = Pattern.compile("(?<=[A-Za-z])\\d+");
        m_colMetaPattern = Pattern.compile("-[A-Za-z]*");
        m_colDefaultPattern = Pattern.compile("(?<=/[\\s*'])[^']*(?=')");
    }


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
                String[] parts = colShorthand.trim().split(":");
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

    //
    // TABLENAME (C1:BIGINT128-UN/'foo', BIGINT) P(C1,4)

    /**
     * Syntax is "[TABLENAME] ([COLNAME:][*]TYPECHAR[size][#[N][U]][$default];)*"
     * Default value for TABLENAME is TABLE.
     * Default value for COLNAME is "C#" where # is the index of the column.
     * Default size, if relevant is 255.
     * Default to nullable and not unique with implied NULL default.
     * Spaces at seperators are trimmed.
     * No quoted string values for now. Trailing semi is optional.
     *
     * Examples:
     * "ORDERS ! l; *i; NOWTIME:T; *S#N$foxy"
     * Table named ORDERS with 4 columns:
     *  C0: BIGINT, nullable
     *  C1: INTEGER, nullable
     *  NOWTIME: TIMESTAMP, nullable
     *  C3: VARCHAR(255), not null, default: "foxy"
     * PKEY is C1,C3 due to the stars
     *
     * "l#U; A C:B60"
     * Table named TABLE with 2 columns:
     *  C0: BIGINT, unique, nullable
     *  A C: VARBINARY(60), nullable
     * NO PKEY
     *
     * @param schema
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
