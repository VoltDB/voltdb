/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package org.voltdb.parser;

import java.util.regex.Pattern;

import org.voltcore.logging.VoltLogger;

public class SQLPatternPartElement extends SQLPatternPart
{
    //===== Private

    String m_leader = null;
    String m_trailer = null;
    String m_separator = null;
    private SQLPatternPart[] m_parts;
    String m_captureLabel = null;

    private static final VoltLogger COMPILER_LOG = new VoltLogger("COMPILER");

    /**
     * Private constructor from multiple strings
     * SQLPat static factory methods should be used for element creation.
     * @param strs  strings used for main part of expression
     */
    SQLPatternPartElement(String[] strs)
    {
        m_parts = new SQLPatternPart[strs.length];
        for (int i = 0; i < strs.length; ++i) {
            m_parts[i] = new SQLPatternPartString(strs[i]);
        }
    }

    /**
     * Private constructor from multiple elements
     * SQLPat static factory methods should be used for element creation.
     * @param parts  pattern parts
     */
    SQLPatternPartElement(SQLPatternPart... parts)
    {
        m_parts = parts;
    }

    /**
     * Private constructor from a single string
     * SQLPat static factory methods should be used for element creation.
     * @param strs  string used for main part of expression
     */
    SQLPatternPartElement(String str)
    {
        m_parts = new SQLPatternPart[] {new SQLPatternPartString(str)};
    }

    //===== Part interface

    @Override
    public String generateExpression(int flagsAdd)
    {
        int flags = m_flags | flagsAdd;
        StringBuilder sb = new StringBuilder();
        if (m_leader != null) {
            sb.append(m_leader);
        }
        // Need a non-capturing group when either an explicit non-capturing group is
        // requested or it is optional. The only case where a non-capturing group isn't
        // needed for an optional is an explicit capture without leading space.
        boolean captureGroup = (flags & SQLPatternFactory.CAPTURE) != 0;
        boolean explicitNonCaptureGroup = !captureGroup && (flags & SQLPatternFactory.GROUP) != 0;
        boolean optional = ((flags & SQLPatternFactory.OPTIONAL) != 0);
        // Suppress the leading space at this level when it should be pushed down to the child.
        boolean leadingSpace = ((flags & SQLPatternFactory.LEADING_SPACE) != 0);
        boolean leadingSpaceToChild = ((flags & SQLPatternFactory.ADD_LEADING_SPACE_TO_CHILD) != 0);
        boolean childLeadingSpace = ((flags & SQLPatternFactory.CHILD_SPACE_SEPARATOR) != 0 ||
                                     (leadingSpace && leadingSpaceToChild));
        boolean nonCaptureGroup = (explicitNonCaptureGroup ||
                                  (optional && (!captureGroup || leadingSpace)));
        boolean innerOptional = optional && captureGroup && !nonCaptureGroup;
        boolean outerOptional = optional && nonCaptureGroup;
        if (nonCaptureGroup) {
            sb.append("(?:");
        }
        if (leadingSpace && !leadingSpaceToChild) {
            // Protect something like an OR sequence by using an inner group
            sb.append("\\s+(?:");
        }
        if (captureGroup) {
            if (m_captureLabel != null) {
                sb.append(String.format("(?<%s>", m_captureLabel));
            }
            else {
                sb.append("(");
            }
        }
        for (int i = 0; i < m_parts.length; ++i) {
            int flagsAddChild = 0;
            if (i > 0) {
                if (m_separator != null) {
                    sb.append(m_separator);
                }
                if (childLeadingSpace) {
                    flagsAddChild |= SQLPatternFactory.LEADING_SPACE;
                }
            }
            else if (childLeadingSpace && leadingSpaceToChild) {
                flagsAddChild |= SQLPatternFactory.LEADING_SPACE;
            }
            sb.append(m_parts[i].generateExpression(flagsAddChild));
        }
        if (captureGroup) {
            sb.append(")");
        }
        if (innerOptional) {
            sb.append("?");
        }
        if (leadingSpace && !leadingSpaceToChild) {
            sb.append(")");
        }
        if (nonCaptureGroup) {
            sb.append(")");
        }
        if (outerOptional) {
            sb.append("?");
        }
        if (m_trailer != null) {
            sb.append(m_trailer);
        }
        return sb.toString();
    }

    @Override
    public void setCaptureLabel(String captureLabel)
    {
        m_captureLabel = captureLabel;
    }

    @Override
    public Pattern compile(String label)
    {
        int reFlags = 0;
        if ((m_flags & SQLPatternFactory.CASE_SENSITIVE) == 0) {
            reFlags |= Pattern.CASE_INSENSITIVE;
        }
        if ((m_flags & SQLPatternFactory.IGNORE_NEW_LINE) == 0) {
            reFlags |= Pattern.DOTALL;
        }
        if ((m_flags & SQLPatternFactory.SINGLE_LINE) == 0) {
            reFlags |= Pattern.MULTILINE;
        }
        String regex = generateExpression(0);
        COMPILER_LOG.debug(String.format("PATTERN: %s: %s", label, regex));
        return Pattern.compile(regex, reFlags);
    }
}
