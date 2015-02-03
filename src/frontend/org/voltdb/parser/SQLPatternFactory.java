/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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


/**
 * Inherit from this class in order to use the static methods without the class name
 * to build patterns.
 */
public class SQLPatternFactory
{
    //===== Flags used internally and from SQLPatternPart's to modify the resulting pattern

    // Non-capturing group
    static int GROUP = 0x0001;
    // Capturing group
    static int CAPTURE = 0x0002;
    // Optional, implies at least a non-capturing group
    static int OPTIONAL = 0x0004;
    // Case-sensitive
    static int CASE_SENSITIVE = 0x0008;
    // Ignore new lines
    static int IGNORE_NEW_LINE = 0x0010;
    // Single line only
    static int SINGLE_LINE = 0x0020;
    // Insert leading whitespace
    static int LEADING_SPACE = 0x0040;
    // Insert leading whitespace before child parts
    static int CHILD_SPACE_SEPARATOR = 0x0080;

    //===== Public

    // Use the SPF wrapper class as a abbreviated namespace to keep pattern construction
    // as concise as possible while not polluting the derived class' namespace.

    public static class SPF
    {
        public static SQLPatternPart statement(SQLPatternPart... parts)
        {
            return makeStatementPart(true, true, true, true, parts);
        }

        public static SQLPatternPart statementLeader(SQLPatternPart... parts)
        {
            return makeStatementPart(true, false, false, false, parts);
        }

        public static SQLPatternPart statementTrailer(SQLPatternPart... parts)
        {
            return makeStatementPart(false, true, true, false, parts);
        }

        public static SQLPatternPart clause(SQLPatternPart... parts)
        {
            SQLPatternPartElement retElem = new SQLPatternPartElement(parts);
            retElem.m_flags |= SQLPatternFactory.GROUP;
            retElem.m_separator = "\\s+";
            return retElem;
        }

        public static SQLPatternPart anyClause()
        {
            return new SQLPatternPartString(".+");
        }

        public static SQLPatternPart capture(SQLPatternPart part)
        {
            return capture(null, part);
        }

        public static SQLPatternPart capture(String captureLabel, SQLPatternPart part)
        {
            // Can only capture SQLPatternPartElement, not SQLPatternPartString.
            assert part instanceof SQLPatternPartElement;
            part.m_flags |= SQLPatternFactory.CAPTURE;
            part.setCaptureLabel(captureLabel);
            return part;
        }

        public static SQLPatternPart optional(SQLPatternPart part)
        {
            part.m_flags |= SQLPatternFactory.OPTIONAL;
            return part;
        }

        public static SQLPatternPart or(SQLPatternPart... parts)
        {
            // Default to outer and inner non-capturing groups.
            SQLPatternPartElement retElem = new SQLPatternPartElement(parts);
            retElem.m_flags |= SQLPatternFactory.GROUP;
            retElem.m_separator = "|";
            return retElem;
        }

        public static SQLPatternPart or(String... strs)
        {
            // Default to outer and inner non-capturing groups.
            SQLPatternPartElement retElem = new SQLPatternPartElement(strs);
            retElem.m_flags |= SQLPatternFactory.GROUP;
            retElem.m_separator = "|";
            return retElem;
        }

        public static SQLPatternPart token(String... strs)
        {
            return or(strs);
        }

        public static SQLPatternPart symbol()
        {
            return new SQLPatternPartElement("[a-z][a-z0-9_]*");
        }

        public static SQLPatternPart ddlName()
        {
            return new SQLPatternPartElement("[\\w$]+");
        }

        public static SQLPatternPart anything()
        {
            return new SQLPatternPartElement(".*");
        }

        public static SQLPatternPart integer()
        {
            return new SQLPatternPartElement("\\d+");
        }
    }

    //===== Private methods

    private static SQLPatternPartElement makeStatementPart(
            boolean beginLine,
            boolean endLine,
            boolean terminated,
            boolean terminatorRequired,
            SQLPatternPart... parts)
    {
        SQLPatternPartElement retElem = new SQLPatternPartElement(parts);
        if (beginLine) {
            retElem.m_leader = "^\\s*";
        }
        else {
            retElem.m_leader = "^.*?";
        }
        if (endLine) {
            if (terminated) {
                if (terminatorRequired) {
                    retElem.m_trailer = "\\s*;\\s*$";
                }
                else {
                    retElem.m_trailer = "\\s*;?\\s*$";
                }
            }
            else {
                retElem.m_trailer = "\\s*$";
            }
        }
        else {
            retElem.m_trailer = ".*$";
        }
        retElem.m_flags |= SQLPatternFactory.CHILD_SPACE_SEPARATOR;
        return retElem;
    }
}
